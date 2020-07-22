import unittest
from os.path import dirname, abspath, join

import pandas as pd
from sqlalchemy import create_engine

from tests.settings import POSTGRESQL_ENGINE
from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.ehr2sql import EHR2SQL
from ukbrest.common.postloader import Postloader
from ukbrest.common.yaml_query import PhenoQuery, EHRQuery


def get_repository_path(data_filename):
    directory = dirname(abspath(__file__))
    directory = join(directory, 'data/')
    return join(directory, data_filename)


def get_full_path(filename):
    root_dir = dirname(dirname(abspath(__file__)))
    return join(root_dir, filename)


class DBTest(unittest.TestCase):
    def setUp(self):
        super(DBTest, self).setUp()

        # wipe postgresql tables
        sql_st = """
        select 'drop table if exists "' || tablename || '" cascade;' from pg_tables where schemaname = 'public';
        """
        db_engine = create_engine(POSTGRESQL_ENGINE)
        tables = pd.read_sql(sql_st, db_engine)

        with db_engine.connect() as con:
            for idx, drop_table_st in tables.iterrows():
                con.execute(drop_table_st.iloc[0])

    def _get_table_contrains(self, table_name, column_query='%%', relationship_query='%%'):
        return """
        select t.relname as table_name, i.relname as index_name, a.attname as column_name
        from pg_class t, pg_class i, pg_index ix, pg_attribute a
        where
            t.oid = ix.indrelid
            and i.oid = ix.indexrelid
            and a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
            and t.relkind = 'r'
            and t.relname = '{table_name}' and a.attname like '{column_query}' and i.relname like '{relationship_query}'
        """.format(
                table_name=table_name,
                column_query=column_query,
                relationship_query=relationship_query,
            )
    def _get_p2sql(self, filename, **kwargs):
        if filename is None:
            csv_file = get_repository_path('pheno2sql/example02.csv')
        elif isinstance(filename, (tuple, list)):
            csv_file = tuple([get_repository_path(f) for f in filename])
        elif isinstance(filename, str):
            csv_file = get_repository_path(filename)
        else:
            raise ValueError('filename unknown type')

        if 'db_uri' not in kwargs:
            kwargs['db_uri'] = POSTGRESQL_ENGINE

        if 'n_columns_per_table' not in kwargs:
            kwargs['n_columns_per_table'] = 2

        return Pheno2SQL(csv_file, **kwargs)

    def _get_ehr2sql(self, gp_dir, hesin_dir, **kwargs):

        if hesin_dir is None:
            hesin_dir = get_repository_path('ehr')
        if gp_dir is None:
            gp_dir = get_repository_path('ehr')
        if 'db_uri' not in kwargs:
            kwargs['db_uri'] = POSTGRESQL_ENGINE

        return EHR2SQL(gp_dir, hesin_dir, **kwargs)

    def _get_postloader(self, **kwargs):
        return Postloader(kwargs.get('db_uri', POSTGRESQL_ENGINE))

    def _get_phenoquery(self, **kwargs):
        db_uri = kwargs.get('db_uri', POSTGRESQL_ENGINE)
        sql_chunksize = kwargs.get('sql_chunksize')
        return PhenoQuery(db_uri, sql_chunksize)

    def _get_ehrquery(self, **kwargs):
        db_uri = kwargs.get('db_uri', POSTGRESQL_ENGINE)
        sql_chunksize = kwargs.get('sql_chunksize')
        return EHRQuery(db_uri, sql_chunksize)
