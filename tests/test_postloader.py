import unittest

import numpy as np
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

        codings = pd.read_sql("select * from codings order by data_coding, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 4

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'Female'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
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

        codings = pd.read_sql("select * from codings order by data_coding, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 2

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 13
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'Date uncertain or unknown'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 13
        assert codings.loc[cidx, 'coding'] == '-3'
        assert codings.loc[cidx, 'meaning'] == 'Preferred not to answer'

    def test_postload_codings_tree_structured(self):
        # prepare
        directory = get_repository_path('postloader/codings03_tree')

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

        codings = pd.read_sql("select * from codings order by data_coding, coding::int, node_id asc", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning', 'node_id', 'parent_id', 'selectable']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 474

        assert np.unique(codings.loc[:, 'data_coding']) == 6

        cidx = 0
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cardiovascular'
        assert codings.loc[cidx, 'node_id'] == 1071
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx += 1
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'respiratory/ent'
        assert codings.loc[cidx, 'node_id'] == 1072
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 10
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cerebrovascular disease'
        assert codings.loc[cidx, 'node_id'] == 1083
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 28
        assert codings.loc[cidx, 'coding'] == '1065'
        assert codings.loc[cidx, 'meaning'] == 'hypertension'
        assert codings.loc[cidx, 'node_id'] == 1081
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 277
        assert codings.loc[cidx, 'coding'] == '1478'
        assert codings.loc[cidx, 'meaning'] == 'cervical spondylosis'
        assert codings.loc[cidx, 'node_id'] == 1541
        assert codings.loc[cidx, 'parent_id'] == 1608
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 473
        assert codings.loc[cidx, 'coding'] == '99999'
        assert codings.loc[cidx, 'meaning'] == 'unclassifiable'
        assert codings.loc[cidx, 'node_id'] == 99999
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

# TODO load tree-structured data
# TODO check primary keys
# TODO check indexes
