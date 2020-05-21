import csv
import os
import re
import sys
import tempfile
from subprocess import Popen, PIPE

from joblib import Parallel, delayed
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.types import TEXT, FLOAT, TIMESTAMP, INT
from sqlalchemy.exc import OperationalError

from ukbrest.common.utils.db import create_table, create_indexes, DBAccess
from ukbrest.common.utils.datagen import get_tmpdir
from ukbrest.common.utils.constants import BGEN_SAMPLES_TABLE, ALL_EIDS_TABLE, EXCLUDE_FROM_ALL_EIDS
from ukbrest.config import logger, SQL_CHUNKSIZE_ENV
from ukbrest.common.utils.misc import get_list
from ukbrest.resources.exceptions import UkbRestSQLExecutionError, UkbRestProgramExecutionError


class LoadSQL(DBAccess):
    def __init__(self, db_uri, n_columns_per_table=sys.maxsize,
                 loading_n_jobs=-1, tmpdir=tempfile.mkdtemp(prefix='ukbrest'),
                 loading_chunksize=5000, sql_chunksize=None,
                 delete_temp_csv=True):
        """

        :param db_uri:
        :param n_columns_per_table:
        :param loading_n_jobs:
        :param tmpdir:
        :param loading_chunksize:
        :param sql_chunksize:
        :param delete_temp_csv:
        """
        super(LoadSQL, self).__init__(db_uri)
        self.n_columns_per_table = n_columns_per_table
        logger.debug("n_columns_per_table set to {}".format(self.n_columns_per_table))
        self.loading_n_jobs = loading_n_jobs
        self.tmpdir = tmpdir
        self.loading_chunksize = loading_chunksize

        self.sql_chunksize = sql_chunksize
        if self.sql_chunksize is None:
            logger.warning('{} was not set, no chunksize for SQL queries, what can lead to '
                           'memory problems.'.format(SQL_CHUNKSIZE_ENV))
        self.delete_temp_csv = delete_temp_csv

    def _run_psql(self, sql_statement, is_file=False):
        current_env = os.environ.copy()
        current_env['PGPASSWORD'] = self.db_pass

        p = Popen(['psql', '-w', '-h', self.db_host, '-p', str(self.db_port),
                   '-U', self.db_user, '-d', self.db_name,
                   '-f' if is_file else '-c', sql_statement],
                  stdout=PIPE, stderr=PIPE, env=current_env)

        stdout_data, stderr_data = p.communicate()
        stdout_data = stdout_data.decode('utf-8')
        stderr_data = stderr_data.decode('utf-8')

        if p.returncode != 0:
            raise UkbRestSQLExecutionError(stdout_data + '\n' + stderr_data)
        elif stderr_data is not None and 'ERROR:' in stderr_data:
            raise UkbRestSQLExecutionError(stderr_data)

    def _load_all_eids(self):
        logger.info('Loading all eids into table {}'.format(ALL_EIDS_TABLE))

        create_table(ALL_EIDS_TABLE,
            columns=[
                'eid bigint NOT NULL',
            ],
            constraints=[
                'pk_{} PRIMARY KEY (eid)'.format(ALL_EIDS_TABLE)
            ],
            db_engine=self._get_db_engine()
         )

        names = set(self._get_table_names())

        names = names - EXCLUDE_FROM_ALL_EIDS

        select_eid_sql = ' UNION DISTINCT '.join(
            'select eid from {}'.format(table_name)
            for table_name in names
        )

        insert_eids_sql = """
            insert into {all_eids_table} (eid)
            (
                {sql_eids}
            )
        """.format(
            all_eids_table=ALL_EIDS_TABLE,
            sql_eids=select_eid_sql
        )

        with self._get_db_engine().connect() as con:
            con.execute(insert_eids_sql)


    def _vacuum(self):
        logger.info('Vacuuming')

        with self._get_db_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute("""
                vacuum analyze;
            """)



