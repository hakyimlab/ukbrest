import io
import shutil
import tempfile
import unittest

import app
from common.genoquery import GenoQuery
from tests.utils import get_repository_path
from utils.datagen import get_temp_file_name
from utils.external import qctool


class TestRestApiGenotype(unittest.TestCase):
    def setUp(self):
        # Load data
        genoq = GenoQuery(get_repository_path('example01'))

        # Configure
        app.app.config['TESTING'] = True
        app.app.config['genoquery'] = genoq
        self.app = app.app.test_client()

    def _save_file(self, response):
        filename = get_temp_file_name('.bgen')
        with open(filename, mode='wb') as f:
            shutil.copyfileobj(io.BytesIO(response.data), f)

        return filename

    def test_genotype_positions_lower_and_upper_limits(self):
        # Prepare
        # Run
        response = self.app.get('/ukbrest/api/v1.0/genotype/1/positions/100/276')

        # Validate
        assert response.status_code == 200, response.status_code

        bgen_file = self._save_file(response)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 3

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 3
        assert results.loc[0, 'rsid'] == 'rs1'
        assert results.loc[1, 'rsid'] == 'rs2'
        assert results.loc[2, 'rsid'] == 'rs3'

        assert results.loc[0, 'allele1'] == 'G'
        assert results.loc[0, 'allele2'] == 'A'

        assert results.loc[1, 'allele1'] == 'G'
        assert results.loc[1, 'allele2'] == 'C'

        assert results.loc[2, 'allele1'] == 'C'
        assert results.loc[2, 'allele2'] == 'A'

        assert results.loc[0, '1.aa'] == 0.7491
        assert results.loc[0, '1.ab'] == 0.0133
        assert results.loc[0, '1.bb'] == 0.2376

        assert results.loc[1, '2.aa'] == 0.8654
        assert results.loc[1, '2.ab'] == 0.1041
        assert results.loc[1, '2.bb'] == 0.0305 # FIXME: I understand that this should be rounded to 0.0306

        assert results.loc[2, '300.aa'] == 0.0828
        assert results.loc[2, '300.ab'] == 0.7751 # FIXME: I understand that this should be rounded to 0.7752
        assert results.loc[2, '300.bb'] == 0.1421

        pos_values = results['pos'].unique()
        assert len(pos_values) == 3
        assert results.loc[0, 'pos'] == 100
        assert results.loc[1, 'pos'] == 181
        assert results.loc[2, 'pos'] == 276

    def test_genotype_positions_lower_limit_only(self):
        # Prepare
        # Run
        response = self.app.get('/ukbrest/api/v1.0/genotype/1/positions/18058')

        # Validate
        assert response.status_code == 200, response.status_code

        bgen_file = self._save_file(response)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 5

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 5
        assert results.loc[0, 'rsid'] == 'rs246'
        assert results.loc[1, 'rsid'] == 'rs247'
        assert results.loc[2, 'rsid'] == 'rs248'
        assert results.loc[3, 'rsid'] == 'rs249'
        assert results.loc[4, 'rsid'] == 'rs250'

        assert results.loc[0, 'allele1'] == 'C'
        assert results.loc[0, 'allele2'] == 'A'

        assert results.loc[1, 'allele1'] == 'T'
        assert results.loc[1, 'allele2'] == 'C'

        assert results.loc[2, 'allele1'] == 'G'
        assert results.loc[2, 'allele2'] == 'C'

        assert results.loc[3, 'allele1'] == 'G'
        assert results.loc[3, 'allele2'] == 'A'

        assert results.loc[4, 'allele1'] == 'T'
        assert results.loc[4, 'allele2'] == 'C'

        assert results.loc[0, '1.aa'] == 0.0537
        assert results.loc[0, '1.ab'] == 0.9160
        assert results.loc[0, '1.bb'] == 0.0302

        assert results.loc[1, '2.aa'] == 0.0698
        assert results.loc[1, '2.ab'] == 0.9117
        assert results.loc[1, '2.bb'] == 0.0186

        assert results.loc[2, '300.aa'] == 0.0826
        assert results.loc[2, '300.ab'] == 0.0316
        assert results.loc[2, '300.bb'] == 0.8858

        assert results.loc[3, '299.aa'] == 0.7988
        assert results.loc[3, '299.ab'] == 0.1666
        assert results.loc[3, '299.bb'] == 0.0345

        assert results.loc[4, '150.aa'] == 0.0773
        assert results.loc[4, '150.ab'] == 0.8683
        assert results.loc[4, '150.bb'] == 0.0544

        pos_values = results['pos'].unique()
        assert len(pos_values) == 5
        assert results.loc[0, 'pos'] == 18058
        assert results.loc[1, 'pos'] == 18139
        assert results.loc[2, 'pos'] == 18211
        assert results.loc[3, 'pos'] == 18294
        assert results.loc[4, 'pos'] == 18389

    def test_genotype_positions_upper_limit_only(self):
        # Prepare
        # Run
        response = self.app.get('/ukbrest/api/v1.0/genotype/1/positions/0/276')

        # Validate
        assert response.status_code == 200, response.status_code

        bgen_file = self._save_file(response)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 3

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 3
        assert results.loc[0, 'rsid'] == 'rs1'
        assert results.loc[1, 'rsid'] == 'rs2'
        assert results.loc[2, 'rsid'] == 'rs3'

        assert results.loc[0, 'allele1'] == 'G'
        assert results.loc[0, 'allele2'] == 'A'

        assert results.loc[1, 'allele1'] == 'G'
        assert results.loc[1, 'allele2'] == 'C'

        assert results.loc[2, 'allele1'] == 'C'
        assert results.loc[2, 'allele2'] == 'A'

        assert results.loc[0, '1.aa'] == 0.7491
        assert results.loc[0, '1.ab'] == 0.0133
        assert results.loc[0, '1.bb'] == 0.2376

        assert results.loc[1, '2.aa'] == 0.8654
        assert results.loc[1, '2.ab'] == 0.1041
        assert results.loc[1, '2.bb'] == 0.0305 # FIXME: I understand that this should be rounded to 0.0306

        assert results.loc[2, '300.aa'] == 0.0828
        assert results.loc[2, '300.ab'] == 0.7751 # FIXME: I understand that this should be rounded to 0.7752
        assert results.loc[2, '300.bb'] == 0.1421

        pos_values = results['pos'].unique()
        assert len(pos_values) == 3
        assert results.loc[0, 'pos'] == 100
        assert results.loc[1, 'pos'] == 181
        assert results.loc[2, 'pos'] == 276

    def test_genotype_positions_using_file(self):
        # Prepare
        positions_file = get_repository_path('example01/positions01.txt')

        # Run
        response = self.app.post('/ukbrest/api/v1.0/genotype/2/positions', data={'file': (open(positions_file, 'rb'), positions_file)})

        # Validate
        assert response.status_code == 200, response.status_code

        bgen_file = self._save_file(response)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 5

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 5
        assert results.loc[0, 'rsid'] == 'rs2000003'
        assert results.loc[1, 'rsid'] == 'rs2000008'
        assert results.loc[2, 'rsid'] == 'rs2000094'
        assert results.loc[3, 'rsid'] == 'rs2000118'
        assert results.loc[4, 'rsid'] == 'rs2000149'

        assert results.loc[0, 'allele1'] == 'C'
        assert results.loc[0, 'allele2'] == 'G'

        assert results.loc[1, 'allele1'] == 'T'
        assert results.loc[1, 'allele2'] == 'A'

        assert results.loc[2, 'allele1'] == 'C'
        assert results.loc[2, 'allele2'] == 'G'

        assert results.loc[3, 'allele1'] == 'T'
        assert results.loc[3, 'allele2'] == 'C'

        assert results.loc[4, 'allele1'] == 'G'
        assert results.loc[4, 'allele2'] == 'T'

        assert results.loc[0, '1.aa'] == 0.7888
        assert results.loc[0, '1.ab'] == 0.1538
        assert results.loc[0, '1.bb'] == 0.0573

        assert results.loc[1, '2.aa'] == 0.8776
        assert results.loc[1, '2.ab'] == 0.0670
        assert results.loc[1, '2.bb'] == 0.0554

        assert results.loc[2, '3.aa'] == 0.0553
        assert results.loc[2, '3.ab'] == 0.0939
        assert results.loc[2, '3.bb'] == 0.8509

        assert results.loc[3, '1.aa'] == 0.1219
        assert results.loc[3, '1.ab'] == 0.8459
        assert results.loc[3, '1.bb'] == 0.0323

        assert results.loc[4, '2.aa'] == 0.0137
        assert results.loc[4, '2.ab'] == 0.0953
        assert results.loc[4, '2.bb'] == 0.8909

        pos_values = results['pos'].unique()
        assert len(pos_values) == 5
        assert results.loc[0, 'pos'] == 300
        assert results.loc[1, 'pos'] == 661
        assert results.loc[2, 'pos'] == 7181
        assert results.loc[3, 'pos'] == 8949
        assert results.loc[4, 'pos'] == 11226

    def test_genotype_rsids_using_file(self):
        # Prepare
        rsids_file = get_repository_path('example01/rsids01.txt')

        # Run
        response = self.app.post('/ukbrest/api/v1.0/genotype/2/rsids', data={'file': (open(rsids_file, 'rb'), rsids_file)})

        # Validate
        assert response.status_code == 200, response.status_code

        bgen_file = self._save_file(response)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 5

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 5
        assert results.loc[0, 'rsid'] == 'rs2000000'
        assert results.loc[1, 'rsid'] == 'rs2000020'
        assert results.loc[2, 'rsid'] == 'rs2000079'
        assert results.loc[3, 'rsid'] == 'rs2000138'
        assert results.loc[4, 'rsid'] == 'rs2000149'

        assert results.loc[0, 'allele1'] == 'A'
        assert results.loc[0, 'allele2'] == 'G'

        assert results.loc[1, 'allele1'] == 'G'
        assert results.loc[1, 'allele2'] == 'C'

        assert results.loc[2, 'allele1'] == 'C'
        assert results.loc[2, 'allele2'] == 'A'

        assert results.loc[3, 'allele1'] == 'A'
        assert results.loc[3, 'allele2'] == 'G'

        assert results.loc[4, 'allele1'] == 'G'
        assert results.loc[4, 'allele2'] == 'T'

        assert results.loc[0, '1.aa'] == 0.9440
        assert results.loc[0, '1.ab'] == 0.0298
        assert results.loc[0, '1.bb'] == 0.0262

        assert results.loc[1, '2.aa'] == 0.1534
        assert results.loc[1, '2.ab'] == 0.7249
        assert results.loc[1, '2.bb'] == 0.1218

        assert results.loc[2, '3.aa'] == 0.9357
        assert results.loc[2, '3.ab'] == 0.0047
        assert results.loc[2, '3.bb'] == 0.0596

        assert results.loc[3, '1.aa'] == 0.8246
        assert results.loc[3, '1.ab'] == 0.0686
        assert results.loc[3, '1.bb'] == 0.1068

        assert results.loc[4, '2.aa'] == 0.0137
        assert results.loc[4, '2.ab'] == 0.0953
        assert results.loc[4, '2.bb'] == 0.8909

        pos_values = results['pos'].unique()
        assert len(pos_values) == 5
        assert results.loc[0, 'pos'] == 100
        assert results.loc[1, 'pos'] == 1623
        assert results.loc[2, 'pos'] == 5925
        assert results.loc[3, 'pos'] == 10447
        assert results.loc[4, 'pos'] == 11226
