import unittest
from os.path import dirname, abspath, join

import pandas as pd
from sqlalchemy import create_engine

from tests.settings import POSTGRESQL_ENGINE


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