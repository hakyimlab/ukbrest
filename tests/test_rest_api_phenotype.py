import io
import json
import unittest
import pandas as pd

import app
from tests.utils import get_repository_path
from common.pheno2sql import Pheno2SQL
from tests.settings import POSTGRESQL_ENGINE


class TestRestApiPhenotype(unittest.TestCase):
    def setUp(self, filename=None):
        # Load data
        if filename is None:
            csv_file = get_repository_path('pheno2sql/example02.csv')
        else:
            csv_file = get_repository_path(filename)

        db_engine = POSTGRESQL_ENGINE

        p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
        p2sql.load_data()

        # Configure
        app.app.config['TESTING'] = True
        app.app.config['pheno2sql'] = p2sql
        self.app = app.app.test_client()

    def test_not_found(self):
        response = self.app.get('/ukbrest/api/v1.0/')
        assert response.status_code == 404, response.status_code

    def test_phenotype_fields(self):
        # Prepare
        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype/fields')

        # Validate
        assert response.status_code == 200, response.status_code

        fields = json.loads(response.data.decode('utf-8'))
        assert len(fields) == 8

    def test_phenotype_query_single_column_format_csv(self):
        # Prepare
        columns = ['c21_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/csv'})

        # Validate
        assert response.status_code == 200, response.status_code

        csv_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), index_col='eid')
        assert csv_file is not None
        assert not csv_file.empty
        assert csv_file.shape == (4, 1)

        assert csv_file.index.name == 'eid'
        assert len(csv_file.index) == 4
        assert all(x in csv_file.index for x in range(1, 4 + 1))

        assert len(csv_file.columns) == len(columns)
        assert all(x in columns for x in csv_file.columns)

        assert csv_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert csv_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert csv_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert csv_file.loc[4, 'c21_0_0'] == 'Option number 4'

    def test_phenotype_query_multiple_column_format_csv(self):
        # Prepare
        columns = ['c21_0_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/csv'})

        # Validate
        assert response.status_code == 200, response.status_code

        csv_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), index_col='eid')
        assert csv_file is not None
        assert not csv_file.empty
        assert csv_file.shape == (4, 2)

        assert csv_file.index.name == 'eid'
        assert len(csv_file.index) == 4
        assert all(x in csv_file.index for x in range(1, 4 + 1))

        assert len(csv_file.columns) == len(columns)
        assert all(x in columns for x in csv_file.columns)

        assert csv_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert csv_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert csv_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert csv_file.loc[4, 'c21_0_0'] == 'Option number 4'

        assert csv_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert csv_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert csv_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert csv_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_multiple_column_format_pheno(self):
        # Prepare
        columns = ['c21_0_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID')
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 2 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == 1
        assert pheno_file.loc[2, 'IID'] == 2
        assert pheno_file.loc[3, 'IID'] == 3
        assert pheno_file.loc[4, 'IID'] == 4

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == 'Option number 4'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_format_pheno_missing_data(self):
        # Prepare
        columns = ['c21_0_0', 'c21_1_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        # na_values='' is necessary to not overwrite NA strings here
        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t',
                                 na_values='', keep_default_na=False, index_col='FID')
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == 1
        assert pheno_file.loc[2, 'IID'] == 2
        assert pheno_file.loc[3, 'IID'] == 3
        assert pheno_file.loc[4, 'IID'] == 4

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == 'Option number 4'

        assert pheno_file.loc[1, 'c21_1_0'] == 'No response'
        assert pheno_file.loc[2, 'c21_1_0'] == 'NA'
        assert pheno_file.loc[3, 'c21_1_0'] == 'Of course'
        assert pheno_file.loc[4, 'c21_1_0'] == 'I don\'t know'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_format_pheno_missing_date(self):
        # Prepare
        self.setUp('pheno2sql/example05_missing_date.csv')

        columns = ['c21_0_0', 'c21_1_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        # na_values='' is necessary to not overwrite NA strings here
        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t',
                                 na_values='', keep_default_na=False, index_col='FID')
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == 1
        assert pheno_file.loc[2, 'IID'] == 2
        assert pheno_file.loc[3, 'IID'] == 3
        assert pheno_file.loc[4, 'IID'] == 4

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == 'NA'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_multiple_column_no_format(self):
        # Prepare
        columns = ['c21_0_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID')
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 2 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == 1
        assert pheno_file.loc[2, 'IID'] == 2
        assert pheno_file.loc[3, 'IID'] == 3
        assert pheno_file.loc[4, 'IID'] == 4

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == 'Option number 4'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_multiple_column_format_not_supported(self):
        # Prepare
        columns = ['c21_0_0', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'application/json'})

        # Validate
        assert response.status_code == 400, response.status_code

        data = json.loads(response.data.decode('utf-8'))
        assert data is not None
        assert 'message' in data
        assert data['message'] is not None

    def test_phenotype_no_columns(self):
        # Prepare
        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype')

        # Validate
        assert response.status_code == 400, response.status_code

    def test_phenotype_query_with_filtering(self):
        # Prepare
        columns = ['c21_0_0', 'c21_2_0', 'c47_0_0', 'c48_0_0']
        filtering = ["c48_0_0 > '2011-01-01'", "c21_2_0 <> ''"]

        parameters = {
            'columns': columns,
            'filters': filtering,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype', query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID')
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape[0] == 2
        assert pheno_file.shape[1] == 4 + 1 # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 2
        assert all(x in pheno_file.index for x in (1, 2))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == 1
        assert pheno_file.loc[2, 'IID'] == 2

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'

        assert pheno_file.loc[1, 'c21_2_0'] == 'Yes'
        assert pheno_file.loc[2, 'c21_2_0'] == 'No'

        assert pheno_file.loc[1, 'c47_0_0'].round(5) == 45.55412
        assert pheno_file.loc[2, 'c47_0_0'].round(5) == -0.55461

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
