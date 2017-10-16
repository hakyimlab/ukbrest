import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from ukbrest.common.postloader import Postloader
from tests.settings import POSTGRESQL_ENGINE
from tests.utils import get_repository_path, DBTest


class PostloaderTest(DBTest):
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
        assert codings.shape[0] == 474 + 2

        assert set(np.unique(codings.loc[:, 'data_coding'])) == {6, 7}

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cardiovascular'
        assert codings.loc[cidx, 'node_id'] == 1071
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'respiratory/ent'
        assert codings.loc[cidx, 'node_id'] == 1072
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 10
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cerebrovascular disease'
        assert codings.loc[cidx, 'node_id'] == 1083
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 28
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '1065'
        assert codings.loc[cidx, 'meaning'] == 'hypertension'
        assert codings.loc[cidx, 'node_id'] == 1081
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 277
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '1478'
        assert codings.loc[cidx, 'meaning'] == 'cervical spondylosis'
        assert codings.loc[cidx, 'node_id'] == 1541
        assert codings.loc[cidx, 'parent_id'] == 1608
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 473
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '99999'
        assert codings.loc[cidx, 'meaning'] == 'unclassifiable'
        assert codings.loc[cidx, 'node_id'] == 99999
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 474
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'
        assert pd.isnull(codings.loc[cidx, 'node_id'])
        assert pd.isnull(codings.loc[cidx, 'parent_id'])
        assert pd.isnull(codings.loc[cidx, 'selectable'])

        cidx = 475
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'
        assert pd.isnull(codings.loc[cidx, 'node_id'])
        assert pd.isnull(codings.loc[cidx, 'parent_id'])
        assert pd.isnull(codings.loc[cidx, 'selectable'])

    def test_postload_codings_check_constrains_exist(self):
        # prepare
        directory = get_repository_path('postloader/codings03_tree')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # Validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        # primary key
        constraint_sql = self._get_table_contrains('codings', relationship_query='pk_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 3
        assert 'data_coding' in columns
        assert 'coding' in columns
        assert 'meaning' in columns

        # index on 'event' column
        constraint_sql = self._get_table_contrains('codings', relationship_query='ix_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 5
        assert 'data_coding' in columns
        assert 'coding' in columns
        assert 'node_id' in columns
        assert 'parent_id' in columns
        assert 'selectable' in columns
