import unittest
import pandas as pd
from sqlalchemy import create_engine

from tests.utils import get_repository_path
from utils.init.pheno2sql import Pheno2SQL
from utils.datagen import get_temp_file_name


class Pheno2SQLTest(unittest.TestCase):
    def test_sqlite_default_values(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = 'sqlite:///tmp.db'

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
        expected_columns = ["eid","21-0.0","21-1.0","21-2.0","31-0.0","34-0.0","46-0.0","47-0.0","48-0.0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, '21-0.0'] == 'Option number 1'
        assert tmp.loc[1, '21-1.0'] == 'No response'
        assert tmp.loc[1, '21-2.0'] == 'Yes'
        assert tmp.loc[1, '31-0.0'] == '2012-01-05'
        assert tmp.loc[1, '34-0.0'] == 21
        assert tmp.loc[1, '46-0.0'] == -9
        assert tmp.loc[1, '47-0.0'].round(5) == 45.55412
        assert tmp.loc[1, '48-0.0'] == '2011-08-14'
        assert tmp.loc[2, '21-0.0'] == 'Option number 2'
        assert tmp.loc[2, '21-1.0'] == ''
        assert tmp.loc[2, '21-2.0'] == 'No'
        assert tmp.loc[2, '31-0.0'] == '2015-12-30'
        assert tmp.loc[2, '34-0.0'] == 12
        assert tmp.loc[2, '46-0.0'] == -2
        assert tmp.loc[2, '47-0.0'].round(5) == -0.55461
        assert tmp.loc[2, '48-0.0'] == '2010-03-29'

    def test_postgresql_default_values(self):
        # Prepare
        csv_file = get_repository_path('pheno2sql/example01.csv')
        db_engine = 'postgresql://test:test@localhost:5432/ukb'

        p2sql = Pheno2SQL(csv_file, db_engine)

        # Run
        p2sql.load_data()

        # Validate
        assert p2sql.db_type == 'postgresql'

        ## Check table exists
        tmp = pd.read_sql("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format('ukb_pheno_00'), create_engine(db_engine))
        assert not tmp.empty

        ## Check columns are correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine))
        expected_columns = ["eid","21-0.0","21-1.0","21-2.0","31-0.0","34-0.0","46-0.0","47-0.0","48-0.0"]
        assert len(tmp.columns) == len(expected_columns)
        assert all(x in expected_columns for x in tmp.columns)

        ## Check data is correct
        tmp = pd.read_sql('select * from ukb_pheno_00', create_engine(db_engine), index_col='eid')
        assert not tmp.empty
        assert tmp.shape[0] == 2
        assert tmp.loc[1, '21-0.0'] == 'Option number 1'
        assert tmp.loc[1, '21-1.0'] == 'No response'
        assert tmp.loc[1, '21-2.0'] == 'Yes'
        assert tmp.loc[1, '31-0.0'].strftime('%Y-%m-%d') == '2012-01-05'
        assert tmp.loc[1, '34-0.0'] == 21
        assert tmp.loc[1, '46-0.0'] == -9
        assert tmp.loc[1, '47-0.0'].round(5) == 45.55412
        assert tmp.loc[1, '48-0.0'].strftime('%Y-%m-%d') == '2011-08-14'
        assert tmp.loc[2, '21-0.0'] == 'Option number 2'
        assert tmp.loc[2, '21-1.0'] == ''
        assert tmp.loc[2, '21-2.0'] == 'No'
        assert tmp.loc[2, '31-0.0'].strftime('%Y-%m-%d') == '2015-12-30'
        assert tmp.loc[2, '34-0.0'] == 12
        assert tmp.loc[2, '46-0.0'] == -2
        assert tmp.loc[2, '47-0.0'].round(5) == -0.55461
        assert tmp.loc[2, '48-0.0'].strftime('%Y-%m-%d') == '2010-03-29'
