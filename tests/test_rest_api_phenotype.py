import io
import json
import unittest

from ukbrest import app
import pandas as pd
from nose.tools import nottest

from tests.settings import POSTGRESQL_ENGINE
from tests.utils import get_repository_path, DBTest
from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.postloader import Postloader


class TestRestApiPhenotype(DBTest):
    def setUp(self, filename=None, load_data=True, wipe_database=True, **kwargs):
        if wipe_database:
            super(TestRestApiPhenotype, self).setUp()
        
        # Load data
        if filename is None:
            csv_file = get_repository_path('pheno2sql/example02.csv')
        elif isinstance(filename, (tuple, list)):
            csv_file = tuple([get_repository_path(f) for f in filename])
        elif isinstance(filename, str):
            csv_file = get_repository_path(filename)
        else:
            raise ValueError('filename unknown type')

        db_engine = POSTGRESQL_ENGINE

        if 'n_columns_per_table' not in kwargs:
            kwargs['n_columns_per_table'] = 2

        p2sql = Pheno2SQL(csv_file, db_engine, **kwargs)
        if load_data:
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

        csv_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), index_col='eid', dtype=str)
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

        csv_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), index_col='eid', dtype=str)
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

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 2 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == 'Option number 4'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_multiple_column_renaming(self):
        # Prepare
        columns = ['c21_0_0 as c21', 'c31_0_0 c31', 'c48_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + ['c21', 'c31', 'c48_0_0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c21'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21'] == 'Option number 4'

        assert pheno_file.loc[1, 'c31'] == '2012-01-05'
        assert pheno_file.loc[2, 'c31'] == '2015-12-30'
        assert pheno_file.loc[3, 'c31'] == '2007-03-19'
        assert pheno_file.loc[4, 'c31'] == '2002-05-09'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2010-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '2011-02-15'

    def test_phenotype_query_filtering_with_column_no_mentioned_in_select(self):
        # Prepare
        columns = ['c21_0_0 as c21', 'c21_2_0 c21_2']
        filtering = ["c46_0_0 < 0", "c48_0_0 > '2011-01-01'"]

        parameters = {
            'columns': columns,
            'filters': filtering,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype', query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape[0] == 2
        assert pheno_file.shape[1] == 2 + 1 # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 2
        assert all(x in pheno_file.index for x in (1, 2))

        expected_columns = ['IID'] + ['c21', 'c21_2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'

        assert pheno_file.loc[1, 'c21'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21'] == 'Option number 2'

        assert pheno_file.loc[1, 'c21_2'] == 'Yes'
        assert pheno_file.loc[2, 'c21_2'] == 'No'

    def test_phenotype_query_multiple_column_integer_values(self):
        # Prepare
        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34_0_0'] == '21'
        assert pheno_file.loc[2, 'c34_0_0'] == '12'
        assert pheno_file.loc[3, 'c34_0_0'] == '1'
        assert pheno_file.loc[4, 'c34_0_0'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9'
        assert pheno_file.loc[2, 'c46_0_0'] == '-2'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

    def test_phenotype_query_multiple_column_integer_values_with_nan(self):
        # Prepare
        self.setUp('pheno2sql/example06_nan_integer.csv')

        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34_0_0'] == '21'
        assert pheno_file.loc[2, 'c34_0_0'] == '12'
        assert pheno_file.loc[3, 'c34_0_0'] == '1'
        assert pheno_file.loc[4, 'c34_0_0'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9'
        assert pheno_file.loc[2, 'c46_0_0'] == 'NA'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

    def test_phenotype_query_multiple_column_integer_values_with_nan_using_columns_renaming_with_as(self):
        # Prepare
        self.setUp('pheno2sql/example06_nan_integer.csv')

        columns = ['c34_0_0 as c34', 'c46_0_0 as c46', 'c47_0_0 as c47']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + ['c34', 'c46', 'c47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34'] == '21'
        assert pheno_file.loc[2, 'c34'] == '12'
        assert pheno_file.loc[3, 'c34'] == '1'
        assert pheno_file.loc[4, 'c34'] == '17'

        assert pheno_file.loc[1, 'c46'] == '-9', pheno_file.loc[1, 'c46']
        assert pheno_file.loc[2, 'c46'] == 'NA'
        assert pheno_file.loc[3, 'c46'] == '-7'
        assert pheno_file.loc[4, 'c46'] == '4'

        assert pheno_file.loc[1, 'c47'] == '45.55412'
        assert pheno_file.loc[2, 'c47'] == '-0.55461'
        assert pheno_file.loc[3, 'c47'] == '-5.32471'
        assert pheno_file.loc[4, 'c47'] == '55.19832'

    def test_phenotype_query_multiple_column_integer_values_with_nan_using_columns_renaming_with_as_uppercase(self):
        # Prepare
        self.setUp('pheno2sql/example06_nan_integer.csv')

        columns = ['c34_0_0 as c34', 'c46_0_0 AS c46', 'c47_0_0 as c47']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + ['c34', 'c46', 'c47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34'] == '21'
        assert pheno_file.loc[2, 'c34'] == '12'
        assert pheno_file.loc[3, 'c34'] == '1'
        assert pheno_file.loc[4, 'c34'] == '17'

        assert pheno_file.loc[1, 'c46'] == '-9', pheno_file.loc[1, 'c46']
        assert pheno_file.loc[2, 'c46'] == 'NA'
        assert pheno_file.loc[3, 'c46'] == '-7'
        assert pheno_file.loc[4, 'c46'] == '4'

        assert pheno_file.loc[1, 'c47'] == '45.55412'
        assert pheno_file.loc[2, 'c47'] == '-0.55461'
        assert pheno_file.loc[3, 'c47'] == '-5.32471'
        assert pheno_file.loc[4, 'c47'] == '55.19832'

    def test_phenotype_query_multiple_column_integer_values_with_nan_using_columns_renaming_with_space(self):
        # Prepare
        self.setUp('pheno2sql/example06_nan_integer.csv')

        columns = ['c34_0_0 as c34', 'c46_0_0 c46', 'c47_0_0 as c47']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + ['c34', 'c46', 'c47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34'] == '21'
        assert pheno_file.loc[2, 'c34'] == '12'
        assert pheno_file.loc[3, 'c34'] == '1'
        assert pheno_file.loc[4, 'c34'] == '17'

        assert pheno_file.loc[1, 'c46'] == '-9', pheno_file.loc[1, 'c46']
        assert pheno_file.loc[2, 'c46'] == 'NA'
        assert pheno_file.loc[3, 'c46'] == '-7'
        assert pheno_file.loc[4, 'c46'] == '4'

        assert pheno_file.loc[1, 'c47'] == '45.55412'
        assert pheno_file.loc[2, 'c47'] == '-0.55461'
        assert pheno_file.loc[3, 'c47'] == '-5.32471'
        assert pheno_file.loc[4, 'c47'] == '55.19832'

    def test_phenotype_query_multiple_column_integer_values_with_nan_using_reg_exp(self):
        # Prepare
        self.setUp('pheno2sql/example06_nan_integer.csv')

        columns = ['c34_0_0 as c34']
        reg_exp_columns = ['c4[67]_0_0']

        parameters = {
            'columns': columns,
            'ecolumns': reg_exp_columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + ['c34', 'c46_0_0', 'c47_0_0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34'] == '21'
        assert pheno_file.loc[2, 'c34'] == '12'
        assert pheno_file.loc[3, 'c34'] == '1'
        assert pheno_file.loc[4, 'c34'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9', pheno_file.loc[1, 'c46']
        assert pheno_file.loc[2, 'c46_0_0'] == 'NA'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

    def test_phenotype_query_multiple_column_create_field_from_integer(self):
        # Prepare
        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0', 'c46_0_0^2 as squared']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 4 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x.split()[-1] in pheno_file.columns for x in expected_columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34_0_0'] == '21'
        assert pheno_file.loc[2, 'c34_0_0'] == '12'
        assert pheno_file.loc[3, 'c34_0_0'] == '1'
        assert pheno_file.loc[4, 'c34_0_0'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9'
        assert pheno_file.loc[2, 'c46_0_0'] == '-2'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

        # square results in float type
        assert pheno_file.loc[1, 'squared'] == '81.0'
        assert pheno_file.loc[2, 'squared'] == '4.0'
        assert pheno_file.loc[3, 'squared'] == '49.0'
        assert pheno_file.loc[4, 'squared'] == '16.0'

    def test_phenotype_query_multiple_column_create_field_from_integer_return_integer(self):
        # Prepare
        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0', 'c46_0_0 + 1 as sum']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 4 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x.split()[-1] in pheno_file.columns for x in expected_columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34_0_0'] == '21'
        assert pheno_file.loc[2, 'c34_0_0'] == '12'
        assert pheno_file.loc[3, 'c34_0_0'] == '1'
        assert pheno_file.loc[4, 'c34_0_0'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9'
        assert pheno_file.loc[2, 'c46_0_0'] == '-2'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

        # square results in float type
        assert pheno_file.loc[1, 'sum'] == '-8'
        assert pheno_file.loc[2, 'sum'] == '-1'
        assert pheno_file.loc[3, 'sum'] == '-6'
        assert pheno_file.loc[4, 'sum'] == '5'

    def test_phenotype_query_multiple_column_create_field_from_float(self):
        # Prepare
        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0', 'c47_0_0^2 as squared']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 4 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x.split()[-1] in pheno_file.columns for x in expected_columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        assert pheno_file.loc[1, 'c34_0_0'] == '21'
        assert pheno_file.loc[2, 'c34_0_0'] == '12'
        assert pheno_file.loc[3, 'c34_0_0'] == '1'
        assert pheno_file.loc[4, 'c34_0_0'] == '17'

        assert pheno_file.loc[1, 'c46_0_0'] == '-9'
        assert pheno_file.loc[2, 'c46_0_0'] == '-2'
        assert pheno_file.loc[3, 'c46_0_0'] == '-7'
        assert pheno_file.loc[4, 'c46_0_0'] == '4'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'
        assert pheno_file.loc[3, 'c47_0_0'] == '-5.32471'
        assert pheno_file.loc[4, 'c47_0_0'] == '55.19832'

        # square results in float type
        assert pheno_file.loc[1, 'squared'] == '2075.1778489744'
        assert pheno_file.loc[2, 'squared'] == '0.3075922521'
        assert pheno_file.loc[3, 'squared'] == '28.3525365841'
        assert pheno_file.loc[4, 'squared'] == '3046.8545308224'

    def test_phenotype_query_multiple_column_create_field_from_str(self):
        # Prepare
        columns = ['c34_0_0', 'c46_0_0', 'c47_0_0', 'c21_0_0', '(c21_0_0 || \' end \' || eid) as result']

        parameters = {
            'columns': columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype',
                                query_string=parameters, headers={'accept': 'text/phenotype'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 5 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x.split()[-1] in pheno_file.columns for x in expected_columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

        # square results in float type
        assert pheno_file.loc[1, 'result'] == 'Option number 1 end 1'
        assert pheno_file.loc[2, 'result'] == 'Option number 2 end 2'
        assert pheno_file.loc[3, 'result'] == 'Option number 3 end 3'
        assert pheno_file.loc[4, 'result'] == 'Option number 4 end 4'

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
                                 na_values='', keep_default_na=False, index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

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
                                 na_values='', keep_default_na=False, index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (4, 3 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 4
        assert all(x in pheno_file.index for x in range(1, 4 + 1))

        expected_columns = ['IID'] + columns
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

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

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
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

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'

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

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str)
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

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'

        assert pheno_file.loc[1, 'c21_2_0'] == 'Yes'
        assert pheno_file.loc[2, 'c21_2_0'] == 'No'

        assert pheno_file.loc[1, 'c47_0_0'] == '45.55412'
        assert pheno_file.loc[2, 'c47_0_0'] == '-0.55461'

        assert pheno_file.loc[1, 'c48_0_0'] == '2011-08-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2016-11-30'

    def test_phenotype_query_columns_with_regular_expression_and_standard_columns(self):
        # Prepare
        self.setUp('pheno2sql/example09_with_arrays.csv')

        columns = ['c21_0_0', 'c48_0_0']
        reg_exp_columns = ['c84_0_\d+']

        parameters = {
            'columns': columns,
            'ecolumns': reg_exp_columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype', query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (5, 5 + 1), pheno_file.shape # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 5
        assert all(x in pheno_file.index for x in range(1, 5 + 1))

        expected_columns = ['IID'] + columns + ['c84_0_0', 'c84_0_1', 'c84_0_2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'
        assert pheno_file.loc[5, 'IID'] == '5'

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == "Option number 4"
        assert pheno_file.loc[5, 'c21_0_0'] == "Option number 5"

        assert pheno_file.loc[1, 'c48_0_0'] == '2010-07-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2017-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2020-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '1990-02-15'
        assert pheno_file.loc[5, 'c48_0_0'] == '1999-10-11'

        assert pheno_file.loc[1, 'c84_0_0'] == '11', pheno_file.loc[1, 'c84_0_0']
        assert pheno_file.loc[2, 'c84_0_0'] == '-21'
        assert pheno_file.loc[3, 'c84_0_0'] == 'NA'
        assert pheno_file.loc[4, 'c84_0_0'] == '41'
        assert pheno_file.loc[5, 'c84_0_0'] == '51'

        assert pheno_file.loc[1, 'c84_0_1'] == '1', pheno_file.loc[1, 'c84_0_1']
        assert pheno_file.loc[2, 'c84_0_1'] == '99'
        assert pheno_file.loc[3, 'c84_0_1'] == '98'
        assert pheno_file.loc[4, 'c84_0_1'] == '-37'
        assert pheno_file.loc[5, 'c84_0_1'] == '36'

        assert pheno_file.loc[1, 'c84_0_2'] == '999'
        assert pheno_file.loc[2, 'c84_0_2'] == '152'
        assert pheno_file.loc[3, 'c84_0_2'] == '-68'
        assert pheno_file.loc[4, 'c84_0_2'] == 'NA'
        assert pheno_file.loc[5, 'c84_0_2'] == '-445'

    def test_phenotype_query_columns_with_regular_expression_only(self):
        # Prepare
        self.setUp('pheno2sql/example09_with_arrays.csv')

        reg_exp_columns = ['c84_0_\d+']

        parameters = {
            'ecolumns': reg_exp_columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype', query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (5, 3 + 1), pheno_file.shape # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 5
        assert all(x in pheno_file.index for x in range(1, 5 + 1))

        expected_columns = ['IID'] + ['c84_0_0', 'c84_0_1', 'c84_0_2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'
        assert pheno_file.loc[5, 'IID'] == '5'

        assert pheno_file.loc[1, 'c84_0_0'] == '11', pheno_file.loc[1, 'c84_0_0']
        assert pheno_file.loc[2, 'c84_0_0'] == '-21'
        assert pheno_file.loc[3, 'c84_0_0'] == 'NA'
        assert pheno_file.loc[4, 'c84_0_0'] == '41'
        assert pheno_file.loc[5, 'c84_0_0'] == '51'

        assert pheno_file.loc[1, 'c84_0_1'] == '1', pheno_file.loc[1, 'c84_0_1']
        assert pheno_file.loc[2, 'c84_0_1'] == '99'
        assert pheno_file.loc[3, 'c84_0_1'] == '98'
        assert pheno_file.loc[4, 'c84_0_1'] == '-37'
        assert pheno_file.loc[5, 'c84_0_1'] == '36'

        assert pheno_file.loc[1, 'c84_0_2'] == '999'
        assert pheno_file.loc[2, 'c84_0_2'] == '152'
        assert pheno_file.loc[3, 'c84_0_2'] == '-68'
        assert pheno_file.loc[4, 'c84_0_2'] == 'NA'
        assert pheno_file.loc[5, 'c84_0_2'] == '-445'

    def test_phenotype_query_columns_pheno2sql_instance_not_loaded(self):
        """This test uses a different Pheno2SQL instance without previous loading"""

        # Prepare
        csv01 = get_repository_path('pheno2sql/example08_01.csv')
        csv02 = get_repository_path('pheno2sql/example08_02.csv')
        csvs = (csv01, csv02)

        # first load data
        self.setUp(csvs)

        # then create another instance without executing load_data method
        self.setUp(csvs, load_data=False, wipe_database=False)

        columns = ['c48_0_0', 'c120_0_0 as c120', 'c150_0_0 c150']
        reg_exp_columns = ['c21_[01]_0', 'c100_\d_0']

        parameters = {
            'columns': columns,
            'ecolumns': reg_exp_columns,
        }

        # Run
        response = self.app.get('/ukbrest/api/v1.0/phenotype', query_string=parameters)

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', na_values='',
                                 keep_default_na=False, index_col='FID', dtype=str)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (5, 8 + 1), pheno_file.shape # plus IID

        assert pheno_file.index.name == 'FID'
        assert len(pheno_file.index) == 5
        assert all(x in pheno_file.index for x in range(1, 5 + 1))

        expected_columns = ['IID'] + ['c21_0_0', 'c21_1_0', 'c48_0_0', 'c120', 'c150', 'c100_0_0', 'c100_1_0', 'c100_2_0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1, 'IID'] == '1'
        assert pheno_file.loc[2, 'IID'] == '2'
        assert pheno_file.loc[3, 'IID'] == '3'
        assert pheno_file.loc[4, 'IID'] == '4'
        assert pheno_file.loc[5, 'IID'] == '5'

        assert pheno_file.loc[1, 'c21_0_0'] == 'Option number 1'
        assert pheno_file.loc[2, 'c21_0_0'] == 'Option number 2'
        assert pheno_file.loc[3, 'c21_0_0'] == 'Option number 3'
        assert pheno_file.loc[4, 'c21_0_0'] == 'Option number 4'
        assert pheno_file.loc[5, 'c21_0_0'] == 'Option number 5'

        assert pheno_file.loc[1, 'c21_1_0'] == 'No response'
        assert pheno_file.loc[2, 'c21_1_0'] == 'NA'
        assert pheno_file.loc[3, 'c21_1_0'] == 'Of course'
        assert pheno_file.loc[4, 'c21_1_0'] == "I don't know"
        assert pheno_file.loc[5, 'c21_1_0'] == 'Maybe'

        assert pheno_file.loc[1, 'c48_0_0'] == '2010-07-14'
        assert pheno_file.loc[2, 'c48_0_0'] == '2017-11-30'
        assert pheno_file.loc[3, 'c48_0_0'] == '2020-01-01'
        assert pheno_file.loc[4, 'c48_0_0'] == '1990-02-15'
        assert pheno_file.loc[5, 'c48_0_0'] == '1999-10-11'

        assert pheno_file.loc[1, 'c100_0_0'] == '-9', pheno_file.loc[1, 'c100_0_0']
        assert pheno_file.loc[2, 'c100_0_0'] == '-2'
        assert pheno_file.loc[3, 'c100_0_0'] == 'NA'
        assert pheno_file.loc[4, 'c100_0_0'] == 'NA'
        assert pheno_file.loc[5, 'c100_0_0'] == 'NA'

        assert pheno_file.loc[1, 'c100_1_0'] == '3', pheno_file.loc[1, 'c100_1_0']
        assert pheno_file.loc[2, 'c100_1_0'] == '3'
        assert pheno_file.loc[3, 'c100_1_0'] == '-4'
        assert pheno_file.loc[4, 'c100_1_0'] == 'NA'
        assert pheno_file.loc[5, 'c100_1_0'] == 'NA'

        assert pheno_file.loc[1, 'c100_2_0'] == 'NA', pheno_file.loc[1, 'c100_2_0']
        assert pheno_file.loc[2, 'c100_2_0'] == '1'
        assert pheno_file.loc[3, 'c100_2_0'] == '-10'
        assert pheno_file.loc[4, 'c100_2_0'] == 'NA'
        assert pheno_file.loc[5, 'c100_2_0'] == 'NA'

    def test_phenotype_query_yaml_get_covariates(self):
        # Prepare
        self.setUp('pheno2sql/example10/example10_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example10/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        covariates:
          - field_name_34: c34_0_0 
          - field_name_47: c47_0_0
        
        fields:
          - instance0: c21_0_0
          - instance1: c21_1_0 
          - instance2: c21_2_0 
        """

        # Run
        response = self.app.post('/ukbrest/api/v1.0/query', data=
            {
                'file': (io.BytesIO(yaml_data), 'data.yaml'),
                'section': 'covariates',
            })

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str,
                                 na_values='', keep_default_na=False)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (5, 2 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert all(x in pheno_file.index for x in (1000010, 1000020, 1000030, 1000040, 1000050))

        expected_columns = ['IID'] + ['field_name_34', 'field_name_47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1000010, 'IID'] == '1000010'
        assert pheno_file.loc[1000010, 'field_name_34'] == '-33'
        assert pheno_file.loc[1000010, 'field_name_47'] == '41.55312'

        assert pheno_file.loc[1000020, 'IID'] == '1000020'
        assert pheno_file.loc[1000020, 'field_name_34'] == '34'
        assert pheno_file.loc[1000020, 'field_name_47'] == '-10.51461'

        assert pheno_file.loc[1000030, 'IID'] == '1000030'
        assert pheno_file.loc[1000030, 'field_name_34'] == '0'
        assert pheno_file.loc[1000030, 'field_name_47'] == '-35.31471'

        assert pheno_file.loc[1000040, 'IID'] == '1000040'
        assert pheno_file.loc[1000040, 'field_name_34'] == '3'
        assert pheno_file.loc[1000040, 'field_name_47'] == '5.20832'

        assert pheno_file.loc[1000050, 'IID'] == '1000050'
        assert pheno_file.loc[1000050, 'field_name_34'] == '-4'
        assert pheno_file.loc[1000050, 'field_name_47'] == 'NA'

    def test_phenotype_query_yaml_get_fields(self):
        # Prepare
        self.setUp('pheno2sql/example10/example10_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example10/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        covariates:
          - field_name_34: c34_0_0 
          - field_name_47: c47_0_0

        fields:
          - instance0: c21_0_0
          - instance1: c21_1_0 
          - instance2: c21_2_0 
        """

        # Run
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'fields',
        })

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str,
                                 na_values='', keep_default_na=False)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (5, 3 + 1)  # plus IID

        assert pheno_file.index.name == 'FID'
        assert all(x in pheno_file.index for x in (1000010, 1000020, 1000030, 1000040, 1000050))

        expected_columns = ['IID'] + ['instance0', 'instance1', 'instance2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1000010, 'IID'] == '1000010'
        assert pheno_file.loc[1000010, 'instance0'] == 'Option number 1'
        assert pheno_file.loc[1000010, 'instance1'] == 'No response'
        assert pheno_file.loc[1000010, 'instance2'] == 'Yes'

        assert pheno_file.loc[1000040, 'IID'] == '1000040'
        assert pheno_file.loc[1000040, 'instance0'] == 'Option number 4'
        assert pheno_file.loc[1000040, 'instance1'] == "I don't know"
        assert pheno_file.loc[1000040, 'instance2'] == 'NA'

    def test_phenotype_query_yaml_filter_samples_with_include_only(self):
        # Prepare
        self.setUp('pheno2sql/example10/example10_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example10/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - c47_0_0  > 0
        
        covariates:
          - field_name_34: c34_0_0 
          - field_name_47: c47_0_0

        fields:
          - instance0: c21_0_0
          - instance1: c21_1_0 
          - instance2: c21_2_0 
        """

        N_EXPECTED_SAMPLES = 2

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'fields',
        })

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str,
                                 na_values='', keep_default_na=False)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 3 + 1), pheno_file.shape # plus IID

        assert pheno_file.index.name == 'FID'
        assert all(x in pheno_file.index for x in (1000010, 1000040)), pheno_file.index.tolist()

        expected_columns = ['IID'] + ['instance0', 'instance1', 'instance2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1000010, 'IID'] == '1000010'
        assert pheno_file.loc[1000010, 'instance0'] == 'Option number 1'
        assert pheno_file.loc[1000010, 'instance1'] == 'No response'
        assert pheno_file.loc[1000010, 'instance2'] == 'Yes'

        assert pheno_file.loc[1000040, 'IID'] == '1000040'
        assert pheno_file.loc[1000040, 'instance0'] == 'Option number 4'
        assert pheno_file.loc[1000040, 'instance1'] == "I don't know"
        assert pheno_file.loc[1000040, 'instance2'] == 'NA'

        #
        # Ask covariates
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
            {
                'file': (io.BytesIO(yaml_data), 'data.yaml'),
                'section': 'covariates',
            })

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_csv(io.StringIO(response.data.decode('utf-8')), sep='\t', index_col='FID', dtype=str,
                                 na_values='', keep_default_na=False)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 2 + 1) # plus IID

        assert pheno_file.index.name == 'FID'
        assert all(x in pheno_file.index for x in (1000010, 1000040))

        expected_columns = ['IID'] + ['field_name_34', 'field_name_47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)
        # column order
        assert pheno_file.columns.tolist()[0] == 'IID'

        assert pheno_file.loc[1000010, 'IID'] == '1000010'
        assert pheno_file.loc[1000010, 'field_name_34'] == '-33'
        assert pheno_file.loc[1000010, 'field_name_47'] == '41.55312'

        assert pheno_file.loc[1000040, 'IID'] == '1000040'
        assert pheno_file.loc[1000040, 'field_name_34'] == '3'
        assert pheno_file.loc[1000040, 'field_name_47'] == '5.20832'

    def test_phenotype_query_yaml_specify_bgenie_format(self):
        # Prepare
        self.setUp('pheno2sql/example10/example10_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example10/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - c47_0_0  > 0

        covariates:
          - field_name_34: c34_0_0 
          - field_name_47: c47_0_0

        fields:
          - instance0: c21_0_0
          - instance1: c21_1_0 
          - instance2: c21_2_0 
        """

        N_EXPECTED_SAMPLES = 5

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'fields',
            'missing_code': '-999',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 3), pheno_file.shape

        expected_columns = ['instance0', 'instance1', 'instance2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'instance0'] == '-999', pheno_file.loc[0, 'instance0']
        assert pheno_file.loc[0, 'instance1'] == '-999'
        assert pheno_file.loc[0, 'instance2'] == '-999'

        assert pheno_file.loc[1, 'instance0'] == '-999'
        assert pheno_file.loc[1, 'instance1'] == '-999'
        assert pheno_file.loc[1, 'instance2'] == '-999'

        assert pheno_file.loc[2, 'instance0'] == 'Option number 4'
        assert pheno_file.loc[2, 'instance1'] == "I don't know"
        assert pheno_file.loc[2, 'instance2'] == '-999'

        assert pheno_file.loc[3, 'instance0'] == 'Option number 1'
        assert pheno_file.loc[3, 'instance1'] == 'No response'
        assert pheno_file.loc[3, 'instance2'] == 'Yes'

        assert pheno_file.loc[4, 'instance0'] == '-999'
        assert pheno_file.loc[4, 'instance1'] == '-999'
        assert pheno_file.loc[4, 'instance2'] == '-999'

        #
        # Ask covariates
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
            {
                'file': (io.BytesIO(yaml_data), 'data.yaml'),
                'section': 'covariates',
                'missing_code': '-999',
            }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)
        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 2)

        expected_columns = ['field_name_34', 'field_name_47']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'field_name_34'] == '-999'
        assert pheno_file.loc[0, 'field_name_47'] == '-999'

        assert pheno_file.loc[1, 'field_name_34'] == '-999'
        assert pheno_file.loc[1, 'field_name_47'] == '-999'

        assert pheno_file.loc[2, 'field_name_34'] == '3'
        assert pheno_file.loc[2, 'field_name_47'] == '5.20832'

        assert pheno_file.loc[3, 'field_name_34'] == '-33'
        assert pheno_file.loc[3, 'field_name_47'] == '41.55312'

        assert pheno_file.loc[4, 'field_name_34'] == '-999'
        assert pheno_file.loc[4, 'field_name_47'] == '-999'

    def test_phenotype_query_yaml_specify_bgenie_format_default_missing_code(self):
        # Prepare
        self.setUp('pheno2sql/example10/example10_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example10/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - c47_0_0  > 0

        covariates:
          - field_name_34: c34_0_0 
          - field_name_47: c47_0_0

        fields:
          - instance0: c21_0_0
          - instance1: c21_1_0 
          - instance2: c21_2_0 
        """

        N_EXPECTED_SAMPLES = 5

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'fields',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 3), pheno_file.shape

        expected_columns = ['instance0', 'instance1', 'instance2']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'instance0'] == 'NA'
        assert pheno_file.loc[0, 'instance1'] == 'NA'
        assert pheno_file.loc[0, 'instance2'] == 'NA'

        assert pheno_file.loc[1, 'instance0'] == 'NA'
        assert pheno_file.loc[1, 'instance1'] == 'NA'
        assert pheno_file.loc[1, 'instance2'] == 'NA'

        assert pheno_file.loc[2, 'instance0'] == 'Option number 4'
        assert pheno_file.loc[2, 'instance1'] == "I don't know"
        assert pheno_file.loc[2, 'instance2'] == 'NA'

        assert pheno_file.loc[3, 'instance0'] == 'Option number 1'
        assert pheno_file.loc[3, 'instance1'] == 'No response'
        assert pheno_file.loc[3, 'instance2'] == 'Yes'

        assert pheno_file.loc[4, 'instance0'] == 'NA'
        assert pheno_file.loc[4, 'instance1'] == 'NA'
        assert pheno_file.loc[4, 'instance2'] == 'NA'

    def test_phenotype_query_yaml_disease_by_coding_first_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=10)

        yaml_data = b"""
        samples_filters:
          - c34_0_0  >= -5
        
        case_control:
          - disease0:
            - 84:
              - coding: [N308]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == '0'      # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'     # 1000030
        assert pheno_file.loc[2, 'disease0'] == '1'      # 1000040
        assert pheno_file.loc[3, 'disease0'] == 'NA'     # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'      # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'      # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_second_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=20)

        yaml_data = b"""
        samples_filters:
          - c34_0_0  >= -5

        case_control:
          - disease0:
            - 84:
              - coding: [E103]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == '1'   # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'   # 1000030
        assert pheno_file.loc[2, 'disease0'] == '1'   # 1000040
        assert pheno_file.loc[3, 'disease0'] == 'NA'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'   # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_different_filter_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=20)

        yaml_data = b"""
        samples_filters:
          - c31_0_0  > '2001-01-01'

        case_control:
          - disease0:
            - 84:
              - coding: [E103]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == 'NA'  # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'disease0'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'disease0'] == '1'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'  # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_filter_includes_nulls_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=20)

        yaml_data = b"""
        samples_filters:
          - c31_0_0 is null or c31_0_0  > '2001-01-01'

        case_control:
          - disease0:
            - 84:
              - coding: [E103]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == '1'  # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'disease0'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'disease0'] == '1'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'  # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_multiple_filters_using_like_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=20)

        yaml_data = b"""
        samples_filters:
          - c31_0_0 is null or c31_0_0  > '2001-01-01'
          - c21_2_0 not like '%%obab%%'

        case_control:
          - disease0:
            - 84:
              - coding: [E103]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == 'NA'  # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'disease0'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'disease0'] == '1'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'  # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_fields_in_filters_are_in_different_tables_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - c21_1_0 not like '%%respo%%'
          - c47_0_0 > 0

        case_control:
          - disease0:
            - 84:
              - coding: [Q750]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == 'NA'  # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'disease0'] == '1'  # 1000040
        assert pheno_file.loc[3, 'disease0'] == 'NA'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == 'NA'  # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)


    def test_phenotype_query_yaml_disease_by_coding_different_data_field_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - lower(c21_2_0) in ('yes', 'no', 'maybe', 'probably')
          - eid not in (select eid from events where field_id = 84 and event in ('Q750'))
    
        case_control:
          - disease0:
            - 85:
              - coding: [1114]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['disease0']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'disease0'] == '1'  # 1000050
        assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'disease0'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'disease0'] == 'NA'  # 1000010
        assert pheno_file.loc[4, 'disease0'] == '1'  # 1000020
        assert pheno_file.loc[5, 'disease0'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_different_disease_name_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - lower(c21_2_0) in ('yes', 'no', 'maybe', 'probably')
          - eid not in (select eid from events where field_id = 84 and event in ('Q750'))

        case_control:
          - another_disease_name:
            - 85:
              - coding: [1114]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['another_disease_name']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'another_disease_name'] == '1'  # 1000050
        assert pheno_file.loc[1, 'another_disease_name'] == 'NA'  # 1000030
        assert pheno_file.loc[2, 'another_disease_name'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'another_disease_name'] == 'NA'  # 1000010
        assert pheno_file.loc[4, 'another_disease_name'] == '1'  # 1000020
        assert pheno_file.loc[5, 'another_disease_name'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_many_codings_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - lower(c21_2_0) in ('yes', 'no', 'maybe')

        case_control:
          - another_disease_name:
            - 85:
              - coding: [1114, 1701]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['another_disease_name']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'another_disease_name'] == 'NA'  # 1000050
        assert pheno_file.loc[1, 'another_disease_name'] == '0'  # 1000030
        assert pheno_file.loc[2, 'another_disease_name'] == 'NA'  # 1000040
        assert pheno_file.loc[3, 'another_disease_name'] == '1'  # 1000010
        assert pheno_file.loc[4, 'another_disease_name'] == '1'  # 1000020
        assert pheno_file.loc[5, 'another_disease_name'] == '0'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

    def test_phenotype_query_yaml_disease_by_coding_many_data_fields_bgenie(self):
        # Prepare
        self.setUp('pheno2sql/example13/example13_diseases.csv',
                   bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
                   sql_chunksize=2, n_columns_per_table=2)

        yaml_data = b"""
        samples_filters:
          - 1 = 1

        case_control:
          - another_disease_name:
            - 85:
              - coding: [978, 1701]
            - 84:
              - coding: [Z876, Z678]
        """

        N_EXPECTED_SAMPLES = 6

        #
        # Ask fields
        #
        response = self.app.post('/ukbrest/api/v1.0/query', data=
        {
            'file': (io.BytesIO(yaml_data), 'data.yaml'),
            'section': 'case_control',
        }, headers={'accept': 'text/bgenie'})

        # Validate
        assert response.status_code == 200, response.status_code

        pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
                                   dtype=str, na_values='', keep_default_na=False)

        assert pheno_file is not None
        assert not pheno_file.empty
        assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape

        expected_columns = ['another_disease_name']
        assert len(pheno_file.columns) == len(expected_columns)
        assert all(x in expected_columns for x in pheno_file.columns)

        assert pheno_file.loc[0, 'another_disease_name'] == '1'  # 1000050
        assert pheno_file.loc[1, 'another_disease_name'] == '1'  # 1000030
        assert pheno_file.loc[2, 'another_disease_name'] == '0'  # 1000040
        assert pheno_file.loc[3, 'another_disease_name'] == '1'  # 1000010
        assert pheno_file.loc[4, 'another_disease_name'] == '0'  # 1000020
        assert pheno_file.loc[5, 'another_disease_name'] == '1'  # 1000070
        # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)

# TODO: if coding is not a list, but a single value, work the same

    # def test_phenotype_query_yaml_disease_by_node_id_bgenie(self):
    #     # Prepare
    #     self.setUp('pheno2sql/example13/example13_diseases.csv',
    #                bgen_sample_file=get_repository_path('pheno2sql/example13/impv2.sample'),
    #                sql_chunksize=2, n_columns_per_table=2)
    #
    #     coding_dir = get_repository_path('postloader/codings05')
    #     pl = Postloader(POSTGRESQL_ENGINE)
    #     pl.load_codings(coding_dir)
    #
    #     yaml_data = b"""
    #     samples_include_only:
    #       - c34_0_0  >= -5
    #
    #     case_control:
    #       - disease0:
    #         - 84:
    #           - node_id: [1501]
    #     """
    #
    #     N_EXPECTED_SAMPLES = 5
    #
    #     #
    #     # Ask fields
    #     #
    #     response = self.app.post('/ukbrest/api/v1.0/query', data=
    #     {
    #         'file': (io.BytesIO(yaml_data), 'data.yaml'),
    #         'section': 'case_control',
    #     }, headers={'accept': 'text/bgenie'})
    #
    #     # Validate
    #     assert response.status_code == 200, response.status_code
    #
    #     pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
    #                                dtype=str, na_values='', keep_default_na=False)
    #
    #     assert pheno_file is not None
    #     assert not pheno_file.empty
    #     assert pheno_file.shape == (N_EXPECTED_SAMPLES, 1), pheno_file.shape
    #
    #     expected_columns = ['disease0']
    #     assert len(pheno_file.columns) == len(expected_columns)
    #     assert all(x in expected_columns for x in pheno_file.columns)
    #
    #     assert pheno_file.loc[0, 'disease0'] == '0'  # 1000050
    #     assert pheno_file.loc[1, 'disease0'] == 'NA'  # 1000030
    #     assert pheno_file.loc[2, 'disease0'] == '1'  # 1000040
    #     assert pheno_file.loc[3, 'disease0'] == 'NA'  # 1000010
    #     assert pheno_file.loc[4, 'disease0'] == '1'  # 1000020
    #     # 1000060 is "not genotyped" (it is not listed in BGEN's samples file)


# TODO to select controls, we need a table reference. It would be good to have, probably, a table with all eid values in all tables
# TODO test with no filters
# TODO rename samples_only to samples_filter
# TODO coding and node_id together
# TODO node_id means recursive, coding means flat; test recursive
# TODO more than one filter
# TODO filter including tables with null values (test inner and outer joins)
# TODO sabe but just csv format, not bgenie

# TODO make covariates to also respect filters:
        # #
        # # Ask covariates
        # #
        # response = self.app.post('/ukbrest/api/v1.0/query', data=
        #     {
        #         'file': (io.BytesIO(yaml_data), 'data.yaml'),
        #         'section': 'covariates',
        #     }, headers={'accept': 'text/bgenie'})
        #
        # # Validate
        # assert response.status_code == 200, response.status_code
        #
        # pheno_file = pd.read_table(io.StringIO(response.data.decode('utf-8')), sep=' ', header=0,
        #                            dtype=str, na_values='', keep_default_na=False)
        # assert pheno_file is not None
        # assert not pheno_file.empty
        # assert pheno_file.shape == (N_EXPECTED_SAMPLES, 2)
        #
        # expected_columns = ['field_name_34', 'field_name_47']
        # assert len(pheno_file.columns) == len(expected_columns)
        # assert all(x in expected_columns for x in pheno_file.columns)
        #
        # assert pheno_file.loc[0, 'field_name_34'] == '-4'
        # assert pheno_file.loc[0, 'field_name_47'] == 'NA'
        #
        # assert pheno_file.loc[1, 'field_name_34'] == 'NA'
        # assert pheno_file.loc[1, 'field_name_47'] == 'NA'
        #
        # assert pheno_file.loc[2, 'field_name_34'] == '3'
        # assert pheno_file.loc[2, 'field_name_47'] == '5.20832'
        #
        # assert pheno_file.loc[3, 'field_name_34'] == 'NA'
        # assert pheno_file.loc[3, 'field_name_47'] == 'NA'
        #
        # assert pheno_file.loc[4, 'field_name_34'] == '34'
        # assert pheno_file.loc[4, 'field_name_47'] == '-10.51461'