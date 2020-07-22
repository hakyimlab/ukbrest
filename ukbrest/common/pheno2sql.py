import csv
import os
import re
import sys
import tempfile
from subprocess import Popen, PIPE
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.types import TEXT, FLOAT, TIMESTAMP, INT
from sqlalchemy.exc import OperationalError

from ukbrest.common.utils.db import create_table, create_indexes, DBAccess
from ukbrest.common.utils.datagen import get_tmpdir
from ukbrest.common.utils.constants import BGEN_SAMPLES_TABLE, ALL_EIDS_TABLE
from ukbrest.config import logger, SQL_CHUNKSIZE_ENV
from ukbrest.common.utils.misc import get_list
from ukbrest.common.loadsql import LoadSQL
from ukbrest.resources.exceptions import UkbRestSQLExecutionError, UkbRestProgramExecutionError

class Pheno2SQL(LoadSQL):
    _RE_COLUMN_NAME_PATTERN = '(?i)c[0-9a-z_]+_[0-9]+_[0-9]+'
    RE_COLUMN_NAME = re.compile('({})'.format(_RE_COLUMN_NAME_PATTERN))

    _RE_FIELD_INFO_PATTERN = '(?i)c(?P<field_id>[0-9a-z_]+)_(?P<instance>[0-9]+)_(?P<array>[0-9]+)'
    RE_FIELD_INFO = re.compile(_RE_FIELD_INFO_PATTERN)

    _RE_FIELD_CODING_PATTERN = '(?i)Uses data-coding (?P<coding>[0-9]+) '
    RE_FIELD_CODING = re.compile(_RE_FIELD_CODING_PATTERN)

    _RE_FULL_COLUMN_NAME_RENAME_PATTERN = '^(?i)\(?(?P<field>{})\)?([ ]+([ ]*as[ ]+)?(?P<rename>[\w_]+))?$'.format(_RE_COLUMN_NAME_PATTERN)
    RE_FULL_COLUMN_NAME_RENAME = re.compile(_RE_FULL_COLUMN_NAME_RENAME_PATTERN)

    def __init__(self, ukb_csvs, db_uri, bgen_sample_file=None, table_prefix='ukb_pheno_',
                 n_columns_per_table=sys.maxsize, loading_n_jobs=-1, tmpdir=tempfile.mkdtemp(prefix='ukbrest'),
                 loading_chunksize=5000, sql_chunksize=None, delete_temp_csv=True):
        """
        :param ukb_csvs: files are loaded in the order they are specified
        :param db_uri:
        :param table_prefix:
        :param n_columns_per_table:
        :param loading_n_jobs:
        :param tmpdir:
        :param loading_chunksize: number of lines to read when loading CSV files to the SQL database.
        :param sql_chunksize: when an SQL query is submited to get phenotypes, this parameteres indicates the
        chunksize (number of rows).
        """

        super(Pheno2SQL, self).__init__(db_uri, n_columns_per_table,
                                        loading_n_jobs, tmpdir,
                                        loading_chunksize, sql_chunksize,
                                        delete_temp_csv)

        if isinstance(ukb_csvs, (tuple, list)):
            self.ukb_csvs = ukb_csvs
        else:
            self.ukb_csvs = (ukb_csvs,)

        self.bgen_sample_file = bgen_sample_file

        self.table_prefix = table_prefix

        # this is a temporary variable that holds information about loading
        self._loading_tmp = {}

        self.csv_files_encoding_file = 'encodings.txt'
        self.csv_files_encoding = 'utf-8'

        # self.delete_temp_csv = delete_temp_csv

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))

    def _get_table_name(self, column_range_index, csv_file_idx):
        return '{}{}_{:02d}'.format(self.table_prefix, csv_file_idx, column_range_index)

    def _chunker(self, seq, size):
        """
        Divides a sequence in chunks according to the given size.
        :param seq:
        :param size:
        :return:
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def _get_db_columns_dtypes(self, ukbcsv_file):
        """
        Returns a Pandas-compatible type list with SQLAlchemy types for each column.

        :param ukbcsv_file:
        :return:
        """

        logger.info('Getting columns types')

        filename = os.path.splitext(ukbcsv_file)[0] + '.html'

        logger.info('Reading data types from {}'.format(filename))
        with open(filename, 'r', encoding='latin1') as f:
            tmp = pd.read_html(f, match='UDI', header=0, index_col=1, flavor='html5lib')

        logger.debug('Filling NaN values')
        df_types = tmp[0].loc[:, 'Type']
        df_types = df_types.fillna(method='ffill')

        df_descriptions = tmp[0].loc[:, 'Description']
        df_descriptions = df_descriptions.fillna(method='ffill')
        del tmp

        db_column_types = {}
        column_types = {}
        column_descriptions = {}
        column_codings = {}

        # open just to get columns
        csv_df = pd.read_csv(ukbcsv_file, index_col=0, header=0, nrows=1)
        columns = csv_df.columns.tolist()
        del csv_df

        logger.debug('Reading columns')
        for col in columns:
            col_type = df_types[col]
            final_db_col_type = TEXT

            if col_type == 'Continuous':
                final_db_col_type = FLOAT

            elif col_type == 'Integer':
                final_db_col_type = INT

            elif col_type in ('Date', 'Time'):
                final_db_col_type = TIMESTAMP

            db_column_types[col] = final_db_col_type
            column_types[self._rename_columns(col)] = col_type
            column_descriptions[self._rename_columns(col)] = df_descriptions[col].split('Uses data-coding ')[0]

            # search for column coding
            coding_matches = re.search(Pheno2SQL.RE_FIELD_CODING, df_descriptions[col])
            if coding_matches is not None:
                column_codings[self._rename_columns(col)] = int(coding_matches.group('coding'))

        return db_column_types, column_types, column_descriptions, column_codings

    def _rename_columns(self, column_name):
        if column_name == 'eid':
            return column_name

        return 'c{}'.format(column_name.replace('.', '_').replace('-', '_'))

    def _create_tables_schema(self, csv_file, csv_file_idx):
        """
        Reads the data types of each data field in csv_file and create the necessary database tables.
        :return:
        """
        logger.info('Creating database tables')

        tmp = pd.read_csv(csv_file, index_col=0, header=0, nrows=1, low_memory=False)
        old_columns = tmp.columns.tolist()
        del tmp
        new_columns = [self._rename_columns(x) for x in old_columns]

        # Remove columns that were previously loaded in other datasets
        if 'existing_col_names' not in self._loading_tmp:
            # dictionary with data-field as key and csv file as value
            columns_and_csv_files = {}
        else:
            columns_and_csv_files = self._loading_tmp['existing_col_names']

        old_columns_clean = []
        new_columns_clean = []

        for old_col_name, new_col_name in tuple(zip(old_columns, new_columns)):
            if new_col_name in columns_and_csv_files:
                corresponding_csv_file = columns_and_csv_files[new_col_name]
                logger.warning(f'Column {new_col_name} already loaded from {corresponding_csv_file}. Skipping.')
                continue

            columns_and_csv_files[new_col_name] = csv_file

            old_columns_clean.append(old_col_name)
            new_columns_clean.append(new_col_name)

        self._loading_tmp['existing_col_names'] = columns_and_csv_files

        # keep only unique columns (not loaded in previous files)
        old_columns = old_columns_clean
        new_columns = new_columns_clean
        all_columns = tuple(zip(old_columns, new_columns))

        # FIXME: check if self.n_columns_per_table is greater than the real number of columns
        self._loading_tmp['chunked_column_names'] = tuple(enumerate(self._chunker(all_columns, self.n_columns_per_table)))
        self._loading_tmp['chunked_table_column_names'] = \
            {self._get_table_name(col_idx, csv_file_idx): [col[1] for col in col_names]
             for col_idx, col_names in self._loading_tmp['chunked_column_names']}

        # get columns dtypes (for PostgreSQL and standard ones)
        db_types_old_column_names, all_fields_dtypes, all_fields_description, all_fields_coding = self._get_db_columns_dtypes(csv_file)
        db_dtypes = {self._rename_columns(k): v for k, v in db_types_old_column_names.items()}
        self._fields_dtypes.update(all_fields_dtypes)

        data_sample = pd.read_csv(csv_file, index_col=0, header=0, nrows=1, dtype=str)
        data_sample = data_sample.rename(columns=self._rename_columns)

        # create fields table
        if csv_file_idx == 0:
            create_table('fields',
                columns=[
                    'column_name text NOT NULL',
                    'table_name text',
                    'field_id text NOT NULL',
                    'description text',
                    'coding bigint',
                    'inst bigint',
                    'arr bigint',
                    'type text NOT NULL',
                ],
                 constraints=[
                     'pk_fields PRIMARY KEY (column_name)'
                 ],
                 db_engine=self._get_db_engine(),
                 drop_if_exists=True
             )

        current_stop = 0
        for column_names_idx, column_names in self._loading_tmp['chunked_column_names']:
            new_columns_names = [x[1] for x in column_names]

            fields_ids = []
            instances = []
            arrays = []
            fields_dtypes = []
            fields_descriptions = []
            fields_codings = []

            for col_name in new_columns_names:
                match = re.match(Pheno2SQL.RE_FIELD_INFO, col_name)

                fields_ids.append(match.group('field_id'))
                instances.append(int(match.group('instance')))
                arrays.append(int(match.group('array')))

                fields_dtypes.append(all_fields_dtypes[col_name])
                fields_descriptions.append(all_fields_description[col_name])

                if col_name in all_fields_coding:
                    fields_codings.append(all_fields_coding[col_name])
                else:
                    fields_codings.append(np.nan)

            # Create main table structure
            table_name = self._get_table_name(column_names_idx, csv_file_idx)
            logger.info('Table {} ({} columns)'.format(table_name, len(new_columns_names)))
            data_sample.loc[[], new_columns_names].to_sql(table_name, self._get_db_engine(), if_exists='replace', dtype=db_dtypes)

            with self._get_db_engine().connect() as conn:
                conn.execute("""
                    ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (eid);
                """.format(table_name=table_name))

            with self._get_db_engine().connect() as conn:
                conn.execute('DROP INDEX ix_{table_name}_eid;'.format(table_name=table_name))

            # Create auxiliary table
            n_column_names = len(new_columns_names)
            current_start = current_stop
            current_stop = current_start + n_column_names

            aux_table = pd.DataFrame({
                'column_name': new_columns_names,
                'field_id': fields_ids,
                'inst': instances,
                'arr': arrays,
                'coding': fields_codings,
                'table_name': table_name,
                'type': fields_dtypes,
                'description': fields_descriptions
            })
            # aux_table = aux_table.set_index('column_name')
            aux_table.to_sql('fields', self._get_db_engine(), index=False, if_exists='append')

    def _get_file_encoding(self, csv_file):
        csv_file_name = os.path.basename(csv_file)

        csv_file_full_path = os.path.realpath(csv_file)
        csv_file_dir = os.path.dirname(csv_file_full_path)

        encoding_file = os.path.join(csv_file_dir, self.csv_files_encoding_file)

        if os.path.isfile(encoding_file):
            enc_file = pd.read_table(encoding_file, index_col=0, header=None, delim_whitespace=True, squeeze=True)

            if not enc_file.index.is_unique:
                logger.error(f'{self.csv_files_encoding_file} has no unique files. Not using the file')
                return self.csv_files_encoding

            if csv_file_name in enc_file.index:
                file_encoding = enc_file.at[csv_file_name]
                logger.info(f'Encoding found in {self.csv_files_encoding_file}: {file_encoding}')

                return file_encoding
        else:
            logger.warning(f'No {self.csv_files_encoding_file} found, assuming {self.csv_files_encoding}')
            return self.csv_files_encoding

    def _save_column_range(self, csv_file, csv_file_idx, column_names_idx, column_names):
        table_name = self._get_table_name(column_names_idx, csv_file_idx)
        output_csv_filename = os.path.join(get_tmpdir(self.tmpdir), table_name + '.csv')
        full_column_names = ['eid'] + [x[0] for x in column_names]

        data_reader = pd.read_csv(csv_file, index_col=0, header=0, usecols=full_column_names,
                                  chunksize=self.loading_chunksize, dtype=str,
                                  encoding=self._get_file_encoding(csv_file))

        new_columns = [x[1] for x in column_names]

        logger.debug('{}'.format(output_csv_filename))

        write_headers = True
        if self.db_type == 'sqlite':
            write_headers = False

        for chunk_idx, chunk in enumerate(data_reader):
            chunk = chunk.rename(columns=self._rename_columns)
            # chunk = self._replace_null_str(chunk)

            if chunk_idx == 0:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_NONNUMERIC, na_rep=np.nan, header=write_headers, mode='w')
            else:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_NONNUMERIC, na_rep=np.nan, header=False, mode='a')


        return table_name, output_csv_filename

    def _create_temporary_csvs(self, csv_file, csv_file_idx):
        logger.info('Writing temporary CSV files')

        self._close_db_engine()
        self.table_csvs = Parallel(n_jobs=self.loading_n_jobs)(
            delayed(self._save_column_range)(csv_file, csv_file_idx, column_names_idx, column_names)
            for column_names_idx, column_names in self._loading_tmp['chunked_column_names']
        )

    def _load_single_csv(self, table_name, file_path):
        logger.info('{} -> {}'.format(file_path, table_name))

        if self.db_type == 'sqlite':
            statement = (
                '.mode csv\n' +
                '.separator ","\n' +
                '.headers on\n' +
                '.import {file_path} {table_name}\n'
            ).format(**locals())

            p = Popen(['sqlite3', self.db_file], stdout=PIPE, stdin=PIPE, stderr=PIPE)
            stdout_data, stderr_data = p.communicate(input=str.encode(statement))

            if p.returncode != 0:
                raise Exception(stdout_data + b'\n' + stderr_data)

            # For each column, set NULL rows with empty strings
            # FIXME: this codes needs refactoring
            for col_name in self._loading_tmp['chunked_table_column_names'][table_name]:
                statement = (
                    'update {table_name} set {col_name} = null where {col_name} == "nan";'
                ).format(**locals())

                p = Popen(['sqlite3', self.db_file], stdout=PIPE, stdin=PIPE, stderr=PIPE)
                stdout_data, stderr_data = p.communicate(input=str.encode(statement))

                if p.returncode != 0:
                    raise Exception(stdout_data + b'\n' + stderr_data)

        elif self.db_type == 'postgresql':
            statement = (
                "\copy {table_name} from '{file_path}' (format csv, header, null ('nan'))"
            ).format(**locals())

            self._run_psql(statement)

            if self.delete_temp_csv:
                logger.debug(f'Removing CSV already loaded: {file_path}')
                os.remove(file_path)

    def _load_csv(self):
        logger.info('Loading CSV files into database')

        if self.db_type != 'sqlite':
            self._close_db_engine()
            # parallel csv loading is only supported in databases different than sqlite
            Parallel(n_jobs=self.loading_n_jobs)(
                delayed(self._load_single_csv)(table_name, file_path)
                for table_name, file_path in self.table_csvs
            )
        else:
            for table_name, file_path in self.table_csvs:
                self._load_single_csv(table_name, file_path)

    def _load_bgen_samples(self):
        if self.bgen_sample_file is None or not os.path.isfile(self.bgen_sample_file):
            logger.warning('BGEN sample file not set or does not exist: {}'.format(self.bgen_sample_file))
            return

        logger.info('Loading BGEN sample file: {}'.format(self.bgen_sample_file))

        create_table(BGEN_SAMPLES_TABLE,
            columns=[
                'index bigint NOT NULL',
                'eid bigint NOT NULL',
            ],
            constraints=[
                'pk_{} PRIMARY KEY (index, eid)'.format(BGEN_SAMPLES_TABLE)
            ],
            db_engine=self._get_db_engine()
         )

        samples_data = pd.read_table(self.bgen_sample_file, sep=' ', header=0, usecols=['ID_1', 'ID_2'], skiprows=[1])
        samples_data.set_index(np.arange(1, samples_data.shape[0] + 1), inplace=True)
        samples_data.drop('ID_2', axis=1, inplace=True)
        samples_data.rename(columns={'ID_1': 'eid'}, inplace=True)

        samples_data.to_sql(BGEN_SAMPLES_TABLE, self._get_db_engine(), if_exists='append')

    def _load_events(self):
        if self.db_type == 'sqlite':
            logger.warning('Events loading is not supported in SQLite')
            return

        logger.info('Loading events table')

        # create table
        db_engine = self._get_db_engine()

        create_table('events',
            columns=[
                'eid bigint NOT NULL',
                'field_id integer NOT NULL',
                'instance integer NOT NULL',
                'event text NOT NULL',
            ],
            constraints=[
                'pk_events PRIMARY KEY (eid, field_id, instance, event)'
            ],
            db_engine=db_engine
         )

        # insert data of categorical multiple fields
        categorical_variables = pd.read_sql("""
            select column_name, field_id, inst, table_name
            from fields
            where type = 'Categorical (multiple)'
        """, self._get_db_engine())

        for (field_id, field_instance), field_data in categorical_variables.groupby(by=['field_id', 'inst']):
            sql_st = """
                insert into events (eid, field_id, instance, event)
                (
                    select distinct *
                    from (
                        select eid, {field_id}, {field_instance}, unnest(array[{field_columns}]) as event
                        from {tables}
                    ) t
                    where t.event is not null
                )
            """.format(
                field_id=field_id,
                field_instance=field_instance,
                field_columns=', '.join([cn for cn in set(field_data['column_name'])]),
                tables=self._create_joins(list(set(field_data['table_name'])), join_type='inner join'),
            )

            with db_engine.connect() as con:
                con.execute(sql_st)

    def _create_constraints(self):
        if self.db_type == 'sqlite':
            logger.warning('Indexes are not supported for SQLite')
            return

        logger.info('Creating table constraints (indexes, primary keys, etc)')

        # bgen's samples table
        if self.bgen_sample_file is not None and os.path.isfile(self.bgen_sample_file):
            create_indexes(BGEN_SAMPLES_TABLE, ('index', 'eid'), db_engine=self._get_db_engine())

        # fields table
        create_indexes('fields', ('field_id', 'inst', 'arr', 'table_name', 'type', 'coding'),
                       db_engine=self._get_db_engine())

        # events table
        create_indexes('events', ('eid', 'field_id', 'instance', 'event', ('field_id', 'event')),
                       db_engine=self._get_db_engine())

    def load_data(self, vacuum=False):
        """
        Load all CSV files specified into the database configured.
        :return:
        """
        logger.info('Loading phenotype data into database')

        try:
            for csv_file_idx, csv_file in enumerate(self.ukb_csvs):
                logger.info('Working on {}'.format(csv_file))

                self._create_tables_schema(csv_file, csv_file_idx)
                self._create_temporary_csvs(csv_file, csv_file_idx)
                self._load_csv()

            self._load_all_eids()
            self._load_bgen_samples()
            self._load_events()
            self._create_constraints()

            if vacuum:
                self._vacuum()

        except OperationalError as e:
            raise UkbRestSQLExecutionError('There was an error with the database: ' + str(e))
        except UnicodeDecodeError as e:
            logger.debug(str(e))
            raise UkbRestProgramExecutionError('Unicode decoding error when reading CSV file. Activate debug to show more details.')

        # delete temporary variable
        del(self._loading_tmp)

        logger.info('Loading finished!')

    def load_sql(self, sql_file):
        self._run_psql(sql_file, is_file=True)
        logger.info(f'SQL file loaded successfully: {sql_file}')

    def initialize(self):
        logger.info('Initializing')

        logger.info('Loading fields dtypes')
        self.init_field_dtypes()

        logger.info('Initialization finished!')
