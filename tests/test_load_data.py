import os
from subprocess import call
import unittest

from tests.utils import get_full_path, get_repository_path

from ukbrest.config import GENOTYPE_PATH_ENV, PHENOTYPE_PATH, GENOTYPE_BGEN_SAMPLE, DB_URI_ENV, LOAD_DATA_VACUUM
from docker.start import _setup_phenotype_path, _setup_genotype_path, _setup_db_uri
from tests.settings import POSTGRESQL_ENGINE


class LoadDataTest(unittest.TestCase):
    def setUp(self):
        self.load_data_path = get_full_path(os.path.join('ukbrest', 'load_data.py'))
        
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
