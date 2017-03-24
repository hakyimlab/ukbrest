import unittest
from os.path import isfile

from common.ukbquery import UKBQuery
from tests.utils import get_repository_path
from utils.external import qctool


class UKBQueryTest(unittest.TestCase):
    def test_query_incl_range_lower_and_upper_limits(self):
        # prepare
        ukbq = UKBQuery(get_repository_path('example01'))

        # run
        bgen_file = ukbq.get_incl_range(chr=1, start=100, stop=290)

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 3

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 3
        assert 'rs1' in rsid_values
        assert 'rs2' in rsid_values
        assert 'rs3' in rsid_values

        assert results.loc[0, 'allele1'] == 'C'
        assert results.loc[0, 'allele2'] == 'T'

        assert results.loc[0, '1.aa'].round(5) == 0.09853
        assert results.loc[0, '1.ab'].round(5) == 0.70301
        assert results.loc[0, '1.bb'].round(5) == 0.19846

        pos_values = results['pos'].unique()
        assert len(pos_values) == 3
        assert 100 in pos_values
        assert 191 in pos_values
        assert 290 in pos_values
