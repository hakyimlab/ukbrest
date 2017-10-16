import unittest

import pandas as pd
from sqlalchemy import create_engine

from ukbrest.common.postloader import Postloader
from tests.settings import POSTGRESQL_ENGINE
from tests.utils import get_repository_path


class PostloaderTest(unittest.TestCase):
    def setUp(self):
        super(PostloaderTest, self).setUp()

        # wipe postgresql tables
        sql_st = """
        select 'drop table if exists "' || tablename || '" cascade;' from pg_tables where schemaname = 'public';
        """
        db_engine = create_engine(POSTGRESQL_ENGINE)
        tables = pd.read_sql(sql_st, db_engine)

        with db_engine.connect() as con:
            for idx, drop_table_st in tables.iterrows():
                con.execute(drop_table_st.iloc[0])

    def test_postload_codings_table_basic(self):
        # prepare
        directory = get_repository_path('postloader/codings01')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by field_id, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['field_id', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 4

        cidx = 0
        assert codings.loc[cidx, 'field_id'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'

        cidx += 1
        assert codings.loc[cidx, 'field_id'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'

        cidx += 1
        assert codings.loc[cidx, 'field_id'] == 9
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'Female'

        cidx += 1
        assert codings.loc[cidx, 'field_id'] == 9
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Male'

    def test_postload_codings_negative_coding(self):
        # prepare
        directory = get_repository_path('postloader/codings02_negative')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by field_id, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['field_id', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 2

        cidx = 0
        assert codings.loc[cidx, 'field_id'] == 13
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'Date uncertain or unknown'

        cidx += 1
        assert codings.loc[cidx, 'field_id'] == 13
        assert codings.loc[cidx, 'coding'] == '-3'
        assert codings.loc[cidx, 'meaning'] == 'Preferred not to answer'


# TODO check primary keys
# TODO check indexes
