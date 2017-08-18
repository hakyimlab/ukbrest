import os, sys
import csv
from urllib.parse import urlparse
from subprocess import Popen, PIPE
import tempfile

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import create_engine
from sqlalchemy.types import TEXT, FLOAT, TIMESTAMP, INT

from ukbrest.common.utils.datagen import get_tmpdir
from ukbrest.config import logger


class Pheno2SQL:
    def __init__(self, ukb_csvs, connection_string, table_prefix='ukb_pheno_', n_columns_per_table=sys.maxsize,
                 n_jobs=-1, tmpdir=tempfile.mkdtemp(prefix='ukbrest'), loading_chunksize=10000, sql_chunksize=None):
        """
        :param ukb_csvs:
        :param connection_string:
        :param table_prefix:
        :param n_columns_per_table:
        :param n_jobs:
        :param tmpdir:
        :param loading_chunksize: number of lines to read when loading CSV files to the SQL database.
        :param sql_chunksize: when an SQL query is submited to get phenotypes, this parameteres indicates the
        chunksize (number of rows).
        """

        if isinstance(ukb_csvs, (tuple, list)):
            self.ukb_csvs = ukb_csvs
        else:
            self.ukb_csvs = (ukb_csvs,)

        self.connection_string = connection_string
        self.db_engine = None

        parse_result = urlparse(self.connection_string)
        self.db_type = parse_result.scheme

        if self.db_type == 'sqlite':
            logger.warning('sqlite does not support parallel loading')
            self.db_file = self.connection_string.split(':///')[-1]
        elif self.db_type == 'postgresql':
            self.db_host = parse_result.hostname
            self.db_port = parse_result.port
            self.db_name = parse_result.path.split('/')[-1]
            self.db_user = parse_result.username
            self.db_pass = parse_result.password

        self.table_prefix = table_prefix
        self.n_columns_per_table = n_columns_per_table
        self.n_jobs = n_jobs
        self.tmpdir = tmpdir
        self.loading_chunksize = loading_chunksize

        self.sql_chunksize = sql_chunksize
        if self.sql_chunksize is None:
            logger.warning('UKBREST_PHENOTYPE_CHUNKSIZE was not set, no chunksize for SQL queries, what can lead to '
                           'memory problems.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))

    def _get_db_engine(self):
        if self.db_engine is None:
            if self.db_type != 'sqlite':
                kargs = {'pool_size': 10}
            else:
                kargs = {}

            self.db_engine = create_engine(self.connection_string, **kargs)

        return self.db_engine

    def _close_db_engine(self):
        if self.db_engine is not None:
            del(self.db_engine)
            self.db_engine = None

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

        return db_column_types, column_types, column_descriptions

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

        all_columns = tuple(zip(old_columns, new_columns))
        # FIXME: check if self.n_columns_per_table is greater than the real number of columns
        self.chunked_column_names = tuple(enumerate(self._chunker(all_columns, self.n_columns_per_table)))
        self.chunked_table_column_names = {self._get_table_name(col_idx, csv_file_idx): [col[1] for col in col_names]
                                           for col_idx, col_names in self.chunked_column_names}

        self._original_db_dtypes, self._fields_dtypes, all_fields_description = self._get_db_columns_dtypes(csv_file)
        self._db_dtypes = {self._rename_columns(k): v for k, v in self._original_db_dtypes.items()}

        data_sample = pd.read_csv(csv_file, index_col=0, header=0, nrows=1, dtype=str)
        data_sample = data_sample.rename(columns=self._rename_columns)

        data_table_if_exist = 'replace'

        if csv_file_idx == 0:
            fields_table_if_exist = ['replace'] + ['append'] * (len(self.chunked_column_names) - 1)
        else:
            fields_table_if_exist = ['append'] * (len(self.chunked_column_names))

        current_stop = 0
        for column_names_idx, column_names in self.chunked_column_names:
            new_columns_names = [x[1] for x in column_names]
            fields_dtypes = [self._fields_dtypes[x] for x in new_columns_names]
            fields_description = [all_fields_description[x] for x in new_columns_names]

            # Create main table structure
            table_name = self._get_table_name(column_names_idx, csv_file_idx)
            logger.info('Table {} ({} columns)'.format(table_name, len(new_columns_names)))
            data_sample.loc[[], new_columns_names].to_sql(table_name, self._get_db_engine(), if_exists=data_table_if_exist, dtype=self._db_dtypes)

            # Create auxiliary table
            n_column_names = len(new_columns_names)
            current_start = current_stop
            current_stop = current_start + n_column_names

            aux_table = pd.DataFrame({
                'field': new_columns_names,
                'table_name': table_name,
                'type': fields_dtypes,
                'description': fields_description
            })
            aux_table = aux_table.set_index('field')
            aux_table.to_sql('fields', self._get_db_engine(), if_exists=fields_table_if_exist[column_names_idx])


    def _save_column_range(self, csv_file, csv_file_idx, column_names_idx, column_names):
        table_name = self._get_table_name(column_names_idx, csv_file_idx)
        output_csv_filename = os.path.join(get_tmpdir(self.tmpdir), table_name + '.csv')
        full_column_names = ['eid'] + [x[0] for x in column_names]

        data_reader = pd.read_csv(csv_file, index_col=0, header=0, usecols=full_column_names,
                                  chunksize=self.loading_chunksize, dtype=str)

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
        self.table_csvs = Parallel(n_jobs=self.n_jobs)(
            delayed(self._save_column_range)(csv_file, csv_file_idx, column_names_idx, column_names)
            for column_names_idx, column_names in self.chunked_column_names
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
            for col_name in self.chunked_table_column_names[table_name]:
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

            current_env = os.environ.copy()
            current_env['PGPASSWORD'] = self.db_pass
            p = Popen(['psql', '-w', '-h', self.db_host, '-p', str(self.db_port),
                       '-U', self.db_user, '-d', self.db_name, '-c', statement],
                      stdout=PIPE, stderr=PIPE, env=current_env)
            stdout_data, stderr_data = p.communicate()

            if p.returncode != 0:
                raise Exception(stdout_data + b'\n' + stderr_data)

    def _load_csv(self):
        logger.info('Loading CSV files into database')

        if self.db_type != 'sqlite':
            self._close_db_engine()
            # parallel csv loading is only supported in databases different than sqlite
            Parallel(n_jobs=self.n_jobs)(
                delayed(self._load_single_csv)(table_name, file_path)
                for table_name, file_path in self.table_csvs
            )
        else:
            for table_name, file_path in self.table_csvs:
                self._load_single_csv(table_name, file_path)

    def load_data(self):
        """
        Load self.ukb_csv into the database configured.
        :return:
        """
        logger.info('Loading phenotype data into database')

        for csv_file_idx, csv_file in enumerate(self.ukb_csvs):
            self._create_tables_schema(csv_file, csv_file_idx)
            self._create_temporary_csvs(csv_file, csv_file_idx)
            self._load_csv()

    def _create_joins(self, tables):
        if len(tables) == 1:
            return tables[0]

        return tables[0] + ' ' + ' '.join(['full outer join {} using (eid) '.format(t) for t in tables[1:]])

    def _get_needed_tables(self, all_columns):
        all_columns_quoted = ["'{}'".format(x.replace("'", "''")) for x in all_columns]

        # FIXME: are parameters correctly escaped by the arg parser?
        tables_needed_df = pd.read_sql(
            'select distinct table_name '
            'from fields '
            'where field in (' + ','.join(all_columns_quoted) + ')',
        self._get_db_engine()).loc[:, 'table_name'].tolist()

        if len(tables_needed_df) == 0:
            raise Exception('Tables not found.')

        return tables_needed_df

    def _get_fields_from_reg_exp(self, ecolumns):
        if ecolumns is None:
            return []

        where_st = ["field ~ '{}'".format(ecol) for ecol in ecolumns]
        select_st = """
            select distinct field
            from fields
            where {}
            order by field
        """.format(' or '.join(where_st))

        return pd.read_sql(select_st, self._get_db_engine()).loc[:, 'field'].tolist()

    def query(self, columns, ecolumns=None, filterings=None, int_to_str=False):
        # get fields from regular expression
        reg_exp_columns = self._get_fields_from_reg_exp(ecolumns)

        # select needed tables to join
        all_columns = ['eid'] + columns + reg_exp_columns
        tables_needed_df = self._get_needed_tables(all_columns)

        # FIXME: are parameters correctly escaped by the arg parser?
        results_iterator = pd.read_sql(
            'select ' + ','.join(all_columns) +
            ' from ' + self._create_joins(tables_needed_df) +
            ((' where ' + ' and '.join(filterings)) if filterings is not None else ''),
        self._get_db_engine(), index_col='eid', chunksize=self.sql_chunksize)

        if self.sql_chunksize is None:
            results_iterator = iter([results_iterator])

        for chunk in results_iterator:
            yield chunk


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='UKBREST.')
    parser.add_argument('ukbcsv', metavar='ukbXXXX.csv', type=str, help='UKB csv file')
    parser.add_argument('db_engine', metavar='SQL_conn_string', type=str, help='SQL connection string. For example: postgresql://test:test@localhost:5432/ukb')
    parser.add_argument('--tmpdir', required=False, type=str, dest='tmpdir', default='/tmp/ukbrest')
    parser.add_argument('--chunksize', required=False, type=int, dest='chunksize', default=20000)
    parser.add_argument('--n_jobs', required=False, type=int, dest='n_jobs', default=-1)

    args = parser.parse_args()

    with Pheno2SQL(args.ukbcsv, args.db_engine) as p2sql:
        p2sql.load_data()
