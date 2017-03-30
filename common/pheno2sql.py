import os, sys
import csv
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import create_engine
from subprocess import Popen, PIPE
from urllib.parse import urlparse


class Pheno2SQL:
    def __init__(self, ukb_csv, db_engine, table_prefix='ukb_pheno_', n_columns_per_table=sys.maxsize,
                 n_jobs=-1, tmpdir='/tmp/ukbrest', chunksize=10000):
        self.ukb_csv = ukb_csv

        self.db_engine = db_engine
        parse_result = urlparse(self.db_engine)
        self.db_type = parse_result.scheme

        if self.db_type == 'sqlite':
            print('Warning: sqlite does not support parallel loading')
            self.db_file = self.db_engine.split(':///')[-1]
        elif self.db_type == 'postgresql':
            parse_result = urlparse(self.db_engine)
            self.db_host = parse_result.hostname
            self.db_port = parse_result.port
            self.db_name = parse_result.path.split('/')[-1]
            self.db_user = parse_result.username
            self.db_pass = parse_result.password

        self.table_prefix = table_prefix
        self.n_columns_per_table = n_columns_per_table
        self.n_jobs = n_jobs
        self.tmpdir = tmpdir
        self.chunksize = chunksize

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in os.listdir(self.tmpdir):
            os.remove(os.path.join(self.tmpdir, f))

    def _get_table_name(self, column_range_index):
        return '{}{:02d}'.format(self.table_prefix, column_range_index)

    def _get_tmpdir(self, tmpdir):
        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)

        return tmpdir

    def _chunker(self, seq, size):
        """
        Divides a sequence in chunks according to the given size.
        :param seq:
        :param size:
        :return:
        """
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def _get_field_types(self, ukbcsv_file):
        """
        Returns a tuple with Pandas-compatible type list and parse date columns.

        :param ukbcsv_file:
        :return:
        """

        filename = os.path.splitext(ukbcsv_file)[0]
        with open(filename + '.html', 'r', encoding='latin1') as f:
            tmp = pd.read_html(f, match='UDI', header=0, index_col=1)

        df = tmp[0].loc[:, 'Type']
        df = df.fillna(method='ffill')
        del tmp

        column_types = {}
        column_date_types = []

        # open just to get columns
        csv_df = pd.read_csv(ukbcsv_file, index_col=0, header=0, nrows=1)
        columns = csv_df.columns.tolist()
        del csv_df

        for col in columns:
            col_type = df[col]
            final_col_type = 'str'

            if col_type in ('Continuous', 'Integer'):
                final_col_type = 'float'

            column_types[col] = final_col_type

            if col_type in ('Date', 'Time'):
                column_date_types.append(col)

        return column_types, column_date_types

    def _rename_columns(self, column_name):
        if column_name == 'eid':
            return column_name

        return 'c{}'.format(column_name.replace('.', '_').replace('-', '_'))

    def _create_tables_schema(self):
        """
        Reads the data types of each data field and create the necessary database tables.
        :return:
        """
        print('  Creating database tables', flush=True)
        tmp = pd.read_csv(self.ukb_csv, index_col=0, header=0, nrows=10, low_memory=False)
        old_columns = tmp.columns.tolist()
        new_columns = [self._rename_columns(x) for x in old_columns]

        self.fields = new_columns

        all_columns = tuple(zip(old_columns, new_columns))
        # FIXME: check if self.n_columns_per_table is greater than the real number of columns
        self.chunked_column_names = tuple(enumerate(self._chunker(all_columns, self.n_columns_per_table)))

        self.column_data_types, date_columns = self._get_field_types(self.ukb_csv)

        data_sample = pd.read_csv(self.ukb_csv, index_col=0, header=0, nrows=1, dtype=self.column_data_types, parse_dates=date_columns)
        data_sample = data_sample.rename(columns=self._rename_columns)

        engine = create_engine(self.db_engine)

        fields_table_if_exist = ['replace'] + ['append'] * (len(self.chunked_column_names) - 1)

        current_stop = 0
        for column_names_idx, column_names in self.chunked_column_names:
            new_columns_names = [x[1] for x in column_names]

            # Create main table structure
            table_name = self._get_table_name(column_names_idx)
            print('    Table {} ({} columns)'.format(table_name, len(new_columns_names)), flush=True)
            data_sample.loc[[], new_columns_names].to_sql(table_name, engine, if_exists='replace')

            # Create auxiliary table
            n_column_names = len(new_columns_names)
            current_start = current_stop
            current_stop = current_start + n_column_names

            aux_table = pd.DataFrame({'field': new_columns_names, 'table_name': table_name})
            aux_table = aux_table.set_index('field')
            aux_table.to_sql('fields', engine, if_exists=fields_table_if_exist[column_names_idx])

    def _save_column_range(self, column_names_idx, column_names):
        table_name = self._get_table_name(column_names_idx)
        output_csv_filename = os.path.join(self._get_tmpdir(self.tmpdir), table_name + '.csv')
        full_column_names = ['eid'] + [x[0] for x in column_names]
        data_reader = pd.read_csv(self.ukb_csv, index_col=0, header=0, usecols=full_column_names,
                                  chunksize=self.chunksize, dtype=self.column_data_types)

        new_columns = [x[1] for x in column_names]

        print('  Writing {}'.format(output_csv_filename), flush=True)

        write_headers = True
        if self.db_type == 'sqlite':
            write_headers = False

        for chunk_idx, chunk in enumerate(data_reader):
            chunk = chunk.rename(columns=self._rename_columns)

            if chunk_idx == 0:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_ALL, header=write_headers, mode='w')
            else:
                chunk.loc[:, new_columns].to_csv(output_csv_filename, quoting=csv.QUOTE_ALL, header=False, mode='a')

        return table_name, output_csv_filename

    def _create_temporary_csvs(self):
        # print('Saving temporary CSV files')
        self.table_csvs = Parallel(n_jobs=self.n_jobs)(
            delayed(self._save_column_range)(column_names_idx, column_names)
            for column_names_idx, column_names in self.chunked_column_names
        )

    def _load_single_csv(self, table_name, file_path):
        print('  Loading into table {}'.format(table_name))

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

        elif self.db_type == 'postgresql':
            statement = (
                "\copy {table_name} from '{file_path}' (format csv, header)"
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
        if self.db_type != 'sqlite':
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
        print('Loading phenotype data into database', flush=True)
        self._create_tables_schema()
        self._create_temporary_csvs()
        self._load_csv()

    def _create_inner_joins(self, tables):
        if len(tables) == 1:
            return tables[0]

        return tables[0] + ' ' + ' '.join(['inner join {} using (eid) '.format(t) for t in tables[1:]])

    def query(self, columns, filterings=None):
        engine = create_engine(self.db_engine)

        # select needed tables to join
        all_columns = ['eid'] + columns
        all_columns_quoted = ["'{}'".format(x) for x in (all_columns)]

        tables_needed_df = pd.read_sql(
            'select distinct table_name '
            'from fields '
            'where field in (' + ','.join(all_columns_quoted) + ')',
        engine).loc[:, 'table_name'].tolist()

        return pd.read_sql(
            'select ' + ','.join(all_columns) +
            ' from ' + self._create_inner_joins(tables_needed_df) +
            ((' where ' + ' and '.join(filterings)) if filterings is not None else ''),
            engine, index_col='eid')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='UKBREST.')
    parser.add_argument('ukbcsv', metavar='ukbXXXX.csv', type=str, help='UKB csv file')
    parser.add_argument('db_engine', metavar='SQL_conn_string', type=str, help='SQL connection string. For example: postgresql://test:test@localhost:5432/ukb')
    parser.add_argument('--tmpdir', required=False, type=str, dest='tmpdir', default='/tmp/ukbrest')
    parser.add_argument('--chunksize', required=False, type=int, dest='chunksize', default=20000)
    parser.add_argument('--n_jobs', required=False, type=int, dest='n_jobs', default=-1)
    # parser.add_argument('--skipdb', dest='skipdb', action='store_true')

    args = parser.parse_args()

    with Pheno2SQL(args.ukbcsv, args.db_engine) as p2sql:
        p2sql.load_data()
