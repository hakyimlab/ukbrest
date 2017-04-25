import os
import unittest

import numpy as np
import pandas as pd
from nose.tools import nottest
from sqlalchemy import create_engine

from tests.settings import POSTGRESQL_ENGINE, SQLITE_ENGINE
from tests.utils import get_repository_path
from ukbrest.common.pheno2sql import Pheno2SQL


class Pheno2SQLTest(unittest.TestCase):
    def test_sqlite_default_values(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'sqlite'

        ## Check table exists
        tmp = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_00'), create_engine(db_engine))
        assert not tmp.empty

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0","c31_0_0","c34_0_0","c46_0_0","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[1, 'c31_0_0'] == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'] == '2011-08-14'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'
        assert tmp.loc[2, 'c31_0_0'] == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert tmp.loc[2, 'c48_0_0'] == '2010-03-29'

    def test_postgresql_default_values(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check table exists
        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
        assert table.iloc[0, 0]

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0","c31_0_0","c34_0_0","c46_0_0","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[1, 'c31_0_0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'
        assert tmp.loc[2, 'c31_0_0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert tmp.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-03-29'

    def test_exit(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = POSTGRESQL_ENGINE

        # Run
        with Pheno2SQL(csv_file, db_engine) as p2sql:
            p2sql.load_data()

        # Validate
        ## Check table exists
        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
        assert table.iloc[0, 0]

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0","c31_0_0","c34_0_0","c46_0_0","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2

        ## Check that temporary files were deleted
        assert len(os.listdir('/tmp/ukbrest')) == 0

    def test_sqlite_less_columns_per_table(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'sqlite'

        ## Check tables exist
        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_00'), create_engine(db_engine))
        assert not table.empty

        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_01'), create_engine(db_engine))
        assert not table.empty

        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_02'), create_engine(db_engine))
        assert not table.empty

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine))
        expected_columns = ["eid","c31_0_0","c34_0_0","c46_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine))
        expected_columns = ["eid","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c31_0_0'] == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'] == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'] == '2011-08-14'
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert tmp.loc[2, 'c48_0_0'] == '2010-03-29'

    def test_postgresql_less_columns_per_table(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check tables exist
        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
        assert table.iloc[0, 0]

        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_01'), create_engine(db_engine))
        assert table.iloc[0, 0]

        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_02'), create_engine(db_engine))
        assert table.iloc[0, 0]

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine))
        expected_columns = ["eid","c31_0_0","c34_0_0","c46_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine))
        expected_columns = ["eid","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c31_0_0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert tmp.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-03-29'

    def test_custom_tmpdir(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = POSTGRESQL_ENGINE

        with Pheno2SQL(csv_file, db_engine, tmpdir='/tmp/custom/directory/here') as p2sql:
            # Run
            p2sql.load_data()

            # Validate
            ## Check table exists
            table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
            assert table.iloc[0, 0]

            ## Check columns are correct
            tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
            expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0","c31_0_0","c34_0_0","c46_0_0","c47_0_0","c48_0_0"]
            assert len(tmp.columns) == len(expected_columns)
            assert all(x in expected_columns for x in tmp.columns)

            ## Check data is correct
            tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
            assert not tmp.empty
            assert tmp.shape[0] == 2

            ## Check that temporary are still there
            assert len(os.listdir('/tmp/custom/directory/here')) > 0

        ## Check that temporary is now clean
        assert len(os.listdir('/tmp/custom/directory/here')) == 0

    def test_sqlite_auxiliary_table_is_created(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'sqlite'

        ## Check tables exist
        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_00'), create_engine(db_engine))
        assert not table.empty

        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_01'), create_engine(db_engine))
        assert not table.empty

        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('ukb_pheno_02'), create_engine(db_engine))
        assert not table.empty

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine))
        expected_columns = ["eid","c31_0_0","c34_0_0","c46_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine))
        expected_columns = ["eid","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check auxiliary table existance
        table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format('fields'), create_engine(db_engine))
        assert not table.empty

        ## Check columns are correct
        tmp = pd.read_sql('select * from fields', create_engine(db_engine))
        expected_columns = ["field", "table_name"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from fields', create_engine(db_engine), index_col='field')
        assert not tmp.empty
        assert tmp.shape[0] == 8
        assert tmp.loc['c21_0_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c21_1_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c21_2_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c31_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c34_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c46_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c47_0_0', 'table_name'] == 'ukb_pheno_02'
        assert tmp.loc['c48_0_0', 'table_name'] == 'ukb_pheno_02'

    def test_postgresql_auxiliary_table_is_created(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3, n_jobs=1)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check tables exist
        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
        assert table.iloc[0, 0]

        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_01'), create_engine(db_engine))
        assert table.iloc[0, 0]

        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_02'), create_engine(db_engine))
        assert table.iloc[0, 0]

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","c21_0_0","c21_1_0","c21_2_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine))
        expected_columns = ["eid","c31_0_0","c34_0_0","c46_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine))
        expected_columns = ["eid","c47_0_0","c48_0_0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check auxiliary table existance
        table = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('fields'), create_engine(db_engine))
        assert table.iloc[0, 0]

        ## Check columns are correct
        tmp = pd.read_sql('select * from fields', create_engine(db_engine))
        expected_columns = ["field", "table_name"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from fields', create_engine(db_engine), index_col='field')
        assert not tmp.empty
        assert tmp.shape[0] == 8
        assert tmp.loc['c21_0_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c21_1_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c21_2_0', 'table_name'] == 'ukb_pheno_00'
        assert tmp.loc['c31_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c34_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c46_0_0', 'table_name'] == 'ukb_pheno_01'
        assert tmp.loc['c47_0_0', 'table_name'] == 'ukb_pheno_02'
        assert tmp.loc['c48_0_0', 'table_name'] == 'ukb_pheno_02'

    def test_sqlite_query_single_table(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=999999)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c48_0_0']

        query_result = p2sql.query(columns)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 4
        assert all(x in query_result.index for x in range(1, 4 + 1))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 4
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'
        assert query_result.loc[3, 'c21_0_0'] == 'Option number 3'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'
        assert query_result.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c48_0_0'] == '2011-08-14'
        assert query_result.loc[2, 'c48_0_0'] == '2016-11-30'
        assert query_result.loc[3, 'c48_0_0'] == '2010-01-01'
        assert query_result.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_postgresql_query_single_table(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=999999)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c48_0_0']

        query_result = p2sql.query(columns)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 4
        assert all(x in query_result.index for x in range(1, 4 + 1))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 4
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'
        assert query_result.loc[3, 'c21_0_0'] == 'Option number 3'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'
        assert query_result.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert query_result.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2016-11-30'
        assert query_result.loc[3, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-01-01'
        assert query_result.loc[4, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-02-15'

    def test_sqlite_query_multiple_tables(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c48_0_0']

        query_result = p2sql.query(columns)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 4
        assert all(x in query_result.index for x in range(1, 4 + 1))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 4
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'
        assert query_result.loc[3, 'c21_0_0'] == 'Option number 3'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'
        assert query_result.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c48_0_0'] == '2011-08-14'
        assert query_result.loc[2, 'c48_0_0'] == '2016-11-30'
        assert query_result.loc[3, 'c48_0_0'] == '2010-01-01'
        assert query_result.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_postgresql_query_multiple_tables(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c48_0_0']

        query_result = p2sql.query(columns)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 4
        assert all(x in query_result.index for x in range(1, 4 + 1))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 4
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'
        assert query_result.loc[3, 'c21_0_0'] == 'Option number 3'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'
        assert query_result.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert query_result.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2016-11-30'
        assert query_result.loc[3, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-01-01'
        assert query_result.loc[4, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-02-15'

    @nottest
    def test_sqlite_query_custom_columns(self):
        # SQLite is very limited when selecting variables, renaming, doing math operations, etc
        pass

    def test_postgresql_query_custom_columns(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0', '(c47_0_0 ^ 2.0) as c47_squared']

        query_result = p2sql.query(columns)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 4
        assert all(x in query_result.index for x in range(1, 4 + 1))

        assert len(query_result.columns) == len(columns)
        assert all(x in ['c21_0_0', 'c21_2_0', 'c47_0_0', 'c47_squared'] for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 4
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'
        assert query_result.loc[3, 'c21_0_0'] == 'Option number 3'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'
        assert query_result.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert query_result.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert query_result.loc[3, 'c47_0_0'].round(5) == -5.32471
        assert query_result.loc[4, 'c47_0_0'].round(5) == 55.19832

        assert query_result.loc[1, 'c47_squared'].round(5) == round(45.55412 ** 2, 5)
        assert query_result.loc[2, 'c47_squared'].round(5) == round((-0.55461) ** 2, 5)
        assert query_result.loc[3, 'c47_squared'].round(5) == round((-5.32471) ** 2, 5)
        assert query_result.loc[4, 'c47_squared'].round(5) == round(55.19832 ** 2, 5)

    def test_sqlite_query_single_filter(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0']
        filter = ['c47_0_0 > 0']

        query_result = p2sql.query(columns, filter)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 2
        assert all(x in query_result.index for x in (1, 4))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 2
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert query_result.loc[4, 'c47_0_0'].round(5) == 55.19832

    def test_postgresql_query_single_filter(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0']
        filter = ['c47_0_0 > 0']

        query_result = p2sql.query(columns, filter)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 2
        assert all(x in query_result.index for x in (1, 4))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 2
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[4, 'c21_0_0'] == 'Option number 4'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert pd.isnull(query_result.loc[4, 'c21_2_0'])

        assert query_result.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert query_result.loc[4, 'c47_0_0'].round(5) == 55.19832

    def test_sqlite_query_multiple_and_filter(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0', 'c48_0_0']
        filter = ["c48_0_0 > '2011-01-01'", "c21_2_0 <> ''"]

        query_result = p2sql.query(columns, filter)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 2
        assert all(x in query_result.index for x in (1, 2))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 2
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'

        assert query_result.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert query_result.loc[2, 'c47_0_0'].round(5) == -0.55461

    def test_postgresql_query_multiple_and_filter(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example02.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Run
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0', 'c48_0_0']
        filter = ["c48_0_0 > '2011-01-01'", "c21_2_0 <> ''"]

        query_result = p2sql.query(columns, filter)

        # Validate
        assert query_result is not None

        assert query_result.index.name == 'eid'
        assert len(query_result.index) == 2
        assert all(x in query_result.index for x in (1, 2))

        assert len(query_result.columns) == len(columns)
        assert all(x in columns for x in query_result.columns)

        assert not query_result.empty
        assert query_result.shape[0] == 2
        assert query_result.loc[1, 'c21_0_0'] == 'Option number 1'
        assert query_result.loc[2, 'c21_0_0'] == 'Option number 2'

        assert query_result.loc[1, 'c21_2_0'] == 'Yes'
        assert query_result.loc[2, 'c21_2_0'] == 'No'

        assert query_result.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert query_result.loc[2, 'c47_0_0'].round(5) == -0.55461

        assert query_result.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert query_result.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2016-11-30'

    def test_sqlite_float_is_empty(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example03.csv')
        db_engine = SQLITE_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3, n_jobs=1)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'sqlite'

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'
        assert tmp.loc[3, 'c21_0_0'] == 'Option number 3'
        assert tmp.loc[3, 'c21_1_0'] == 'Of course'
        assert tmp.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(tmp.loc[4, 'c21_2_0'])

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c31_0_0'] == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'] == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2
        assert tmp.loc[3, 'c31_0_0'] == '2007-03-19'
        assert int(tmp.loc[3, 'c34_0_0']) == 1
        assert int(tmp.loc[3, 'c46_0_0']) == -7

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        # FIXME: this is strange, data type in this particular case needs np.round
        assert np.round(tmp.loc[1, 'c47_0_0'], 5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'] == '2011-08-14'
        assert tmp.loc[2, 'c47_0_0'] == -0.55461
        assert tmp.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pd.isnull(tmp.loc[3, 'c47_0_0'])
        assert tmp.loc[3, 'c48_0_0'] == '2010-01-01'

    def test_postgresql_float_is_empty(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example03.csv')
        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3, n_jobs=1)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'
        assert tmp.loc[3, 'c21_0_0'] == 'Option number 3'
        assert tmp.loc[3, 'c21_1_0'] == 'Of course'
        assert tmp.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(tmp.loc[4, 'c21_2_0'])


        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c31_0_0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2
        assert tmp.loc[3, 'c31_0_0'].strftime('%Y-%m-%d') == '2007-03-19'
        assert int(tmp.loc[3, 'c34_0_0']) == 1
        assert int(tmp.loc[3, 'c46_0_0']) == -7

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert tmp.loc[2, 'c48_0_0'].strftime('%Y-%m-%d') == '2016-11-30'
        assert pd.isnull(tmp.loc[3, 'c47_0_0'])
        assert tmp.loc[3, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-01-01'

    def test_postgresql_timestamp_is_empty(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example04.csv')
        db_engine = 'postgresql://test:test@localhost:5432/ukb'

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3, n_jobs=1)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c21_0_0'] == 'Option number 1'
        assert tmp.loc[1, 'c21_1_0'] == 'No response'
        assert tmp.loc[1, 'c21_2_0'] == 'Yes'
        assert tmp.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pd.isnull(tmp.loc[2, 'c21_1_0'])
        assert tmp.loc[2, 'c21_2_0'] == 'No'
        assert tmp.loc[3, 'c21_0_0'] == 'Option number 3'
        assert tmp.loc[3, 'c21_1_0'] == 'Of course'
        assert tmp.loc[3, 'c21_2_0'] == 'Maybe'
        assert pd.isnull(tmp.loc[4, 'c21_2_0'])

        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c31_0_0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        assert int(tmp.loc[2, 'c46_0_0']) == -2
        assert tmp.loc[3, 'c31_0_0'].strftime('%Y-%m-%d') == '2007-03-19'
        assert int(tmp.loc[3, 'c34_0_0']) == 1
        assert int(tmp.loc[3, 'c46_0_0']) == -7
        assert pd.isnull(tmp.loc[4, 'c31_0_0'])

        tmp = pd.read_sql('select * from ukb_pheno_02', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert tmp.loc[1, 'c48_0_0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert tmp.loc[2, 'c47_0_0'].round(5) == -0.55461
        assert pd.isnull(tmp.loc[2, 'c48_0_0'])
        assert tmp.loc[3, 'c47_0_0'].round(5) == -5.32471
        assert tmp.loc[3, 'c48_0_0'].strftime('%Y-%m-%d') == '2010-01-01'

    def test_postgresql_integer_is_nan(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example06_nan_integer.csv')
        db_engine = 'postgresql://test:test@localhost:5432/ukb'

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=3, n_jobs=1)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_01', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 4
        assert tmp.loc[1, 'c31_0_0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert int(tmp.loc[1, 'c34_0_0']) == 21
        assert int(tmp.loc[1, 'c46_0_0']) == -9
        assert tmp.loc[2, 'c31_0_0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert int(tmp.loc[2, 'c34_0_0']) == 12
        pd.isnull(tmp.loc[2, 'c46_0_0'])
        assert tmp.loc[3, 'c31_0_0'].strftime('%Y-%m-%d') == '2007-03-19'
        assert int(tmp.loc[3, 'c34_0_0']) == 1
        assert int(tmp.loc[3, 'c46_0_0']) == -7
        assert pd.isnull(tmp.loc[4, 'c31_0_0'])
