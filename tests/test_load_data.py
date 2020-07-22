import os
from subprocess import call
import unittest

from tests.utils import get_full_path, get_repository_path, DBTest

from ukbrest.config import GENOTYPE_PATH_ENV, PHENOTYPE_PATH, GENOTYPE_BGEN_SAMPLE, DB_URI_ENV, LOAD_DATA_VACUUM
from docker.start import _setup_phenotype_path, _setup_genotype_path, _setup_db_uri
from tests.settings import POSTGRESQL_ENGINE
from ukbrest.common.ehr2sql import EHR2SQL


class LoadDataTest(DBTest):

    def setUp(self, wipe_data=True):
        self.load_data_path = get_full_path(os.path.join('ukbrest', 'load_data.py'))
        self.basic_2sql_config = {'db_uri': POSTGRESQL_ENGINE,
                             'sql_chunksize': None}
        self.ehr_path = get_repository_path("ehr")
        self.ehr_missing_path = get_repository_path("ehr_missing")
        if wipe_data:
            super(LoadDataTest, self).setUp()

    @unittest.skip
    def test_basic_call(self):
        os.environ[GENOTYPE_PATH_ENV] = get_repository_path('pheno2sql/example12/')
        os.environ[PHENOTYPE_PATH] = get_repository_path('pheno2sql/example12/')
        os.environ[DB_URI_ENV] = POSTGRESQL_ENGINE
        os.environ[GENOTYPE_BGEN_SAMPLE] = 'impv2.sample'
        os.environ[LOAD_DATA_VACUUM] = "yes"

        _setup_genotype_path()
        _setup_phenotype_path()
        _setup_db_uri()

        options = [
            'python',
            self.load_data_path,
        ]

        return_code = call(options)
        assert return_code == 0

    def test_load_ehr_no_path(self):
        db_uri = POSTGRESQL_ENGINE
        with self.assertRaises(ValueError):
            ehr2sql = EHR2SQL(None, None, db_uri=db_uri)

    def test_load_ehr_single_path_gp(self):
        ehr2sql = self._get_ehr2sql(self.ehr_path, None,
                                    **self.basic_2sql_config)
        ehr2sql.load_data()

    def test_load_ehr_single_path_hesin(self):
        ehr2sql = self._get_ehr2sql(None, self.ehr_path,
                                    **self.basic_2sql_config)
        ehr2sql.load_data()

    def test_load_ehr_some_files_missing(self):
        ehr2sql = self._get_ehr2sql(self.ehr_missing_path,
                                    self.ehr_missing_path,
                                    **self.basic_2sql_config)
        ehr2sql.load_data()

    def test_load_ehr_all_files_in_place(self):
        ehr2sql = self._get_ehr2sql(self.ehr_path,
                                    self.ehr_path,
                                    **self.basic_2sql_config)
        ehr2sql.load_data()

    def test_load_ehr_tiny_loading_chunksize(self):
        self.basic_2sql_config['loading_chunksize'] = 1
        ehr2sql = self._get_ehr2sql(self.ehr_path,
                                    self.ehr_path,
                                    **self.basic_2sql_config)
        ehr2sql.load_data()