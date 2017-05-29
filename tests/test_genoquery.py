import os
import unittest
import shutil
from os.path import isfile, isdir

from ukbrest.common.utils.external import qctool

from tests.utils import get_repository_path
from ukbrest.common.genoquery import GenoQuery


class UKBQueryTest(unittest.TestCase):
    def test_query_incl_range_lower_and_upper_limits_at_beginning(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_range(chr=1, start=100, stop=276)

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
        assert results.loc[1, '2.bb'] == 0.0306

        assert results.loc[2, '300.aa'] == 0.0828
        assert results.loc[2, '300.ab'] == 0.7752
        assert results.loc[2, '300.bb'] == 0.1421

        pos_values = results['pos'].unique()
        assert len(pos_values) == 3
        assert results.loc[0, 'pos'] == 100
        assert results.loc[1, 'pos'] == 181
        assert results.loc[2, 'pos'] == 276

    def test_query_incl_range_lower_and_upper_limits_at_end(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_range(chr=1, start=18058, stop=18389)

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

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
        assert results.loc[1, '2.ab'] == 0.9116
        assert results.loc[1, '2.bb'] == 0.0186

        assert results.loc[2, '300.aa'] == 0.0826
        assert results.loc[2, '300.ab'] == 0.0316
        assert results.loc[2, '300.bb'] == 0.8858

        assert results.loc[3, '299.aa'] == 0.7988
        assert results.loc[3, '299.ab'] == 0.1666
        assert results.loc[3, '299.bb'] == 0.0346

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

    def test_query_incl_range_lower_limit_only(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_range(chr=1, start=18058)

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

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
        assert results.loc[1, '2.ab'] == 0.9116
        assert results.loc[1, '2.bb'] == 0.0186

        assert results.loc[2, '300.aa'] == 0.0826
        assert results.loc[2, '300.ab'] == 0.0316
        assert results.loc[2, '300.bb'] == 0.8858

        assert results.loc[3, '299.aa'] == 0.7988
        assert results.loc[3, '299.ab'] == 0.1666
        assert results.loc[3, '299.bb'] == 0.0346

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

    def test_query_incl_range_upper_limit_only(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_range(chr=1, stop=276)

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
        assert results.loc[1, '2.bb'] == 0.0306

        assert results.loc[2, '300.aa'] == 0.0828
        assert results.loc[2, '300.ab'] == 0.7752
        assert results.loc[2, '300.bb'] == 0.1421

        pos_values = results['pos'].unique()
        assert len(pos_values) == 3
        assert results.loc[0, 'pos'] == 100
        assert results.loc[1, 'pos'] == 181
        assert results.loc[2, 'pos'] == 276

    def test_query_incl_range_using_file(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))
        # positions are not ordered in the file, but they should be returned ordered
        positions_file = get_repository_path('example01/positions01.txt')

        # run
        bgen_file = genoq.get_incl_range_from_file(2, positions_file)

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

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

        assert results.loc[0, '1.aa'] == 0.7889
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

    def test_query_incl_rsids_single(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_rsids(2, ['rs2000082'])

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 1

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 1
        assert results.loc[0, 'rsid'] == 'rs2000082'

        assert results.loc[0, 'allele1'] == 'A'
        assert results.loc[0, 'allele2'] == 'T'

        assert results.loc[0, '1.aa'] == 0.0016
        assert results.loc[0, '1.ab'] == 0.8613
        assert results.loc[0, '1.bb'] == 0.1371

        assert results.loc[0, '300.aa'] == 0.0234
        assert results.loc[0, '300.ab'] == 0.0148
        assert results.loc[0, '300.bb'] == 0.9618

        pos_values = results['pos'].unique()
        assert len(pos_values) == 1
        assert results.loc[0, 'pos'] == 6192

    def test_query_incl_rsids_multiple(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))

        # run
        bgen_file = genoq.get_incl_rsids(2, ['rs2000082', 'rs2000142'])

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 2

        rsid_values = results['rsid'].unique()
        assert len(rsid_values) == 2
        assert results.loc[0, 'rsid'] == 'rs2000082'
        assert results.loc[1, 'rsid'] == 'rs2000142'

        assert results.loc[0, 'allele1'] == 'A'
        assert results.loc[0, 'allele2'] == 'T'

        assert results.loc[1, 'allele1'] == 'T'
        assert results.loc[1, 'allele2'] == 'G'

        assert results.loc[0, '1.aa'] == 0.0016
        assert results.loc[0, '1.ab'] == 0.8613
        assert results.loc[0, '1.bb'] == 0.1371

        assert results.loc[0, '300.aa'] == 0.0234
        assert results.loc[0, '300.ab'] == 0.0148
        assert results.loc[0, '300.bb'] == 0.9618

        assert results.loc[1, '1.aa'] == 0.9619
        assert results.loc[1, '1.ab'] == 0.0015
        assert results.loc[1, '1.bb'] == 0.0366

        assert results.loc[1, '300.aa'] == 0.0185
        assert results.loc[1, '300.ab'] == 0.1408
        assert results.loc[1, '300.bb'] == 0.8407

        pos_values = results['pos'].unique()
        assert len(pos_values) == 2
        assert results.loc[0, 'pos'] == 6192
        assert results.loc[1, 'pos'] == 10750

    def test_query_incl_rsids_using_file(self):
        # prepare
        genoq = GenoQuery(get_repository_path('example01'))
        # rsids are not ordered in the file, but they should be returned ordered
        rsids_file = get_repository_path('example01/rsids01.txt')

        # run
        bgen_file = genoq.get_incl_rsids(2, [rsids_file])

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

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

    def test_query_incl_range_temp_directory(self):
        # prepare
        shutil.rmtree('/tmp/ukbrest_different/', ignore_errors=True)
        genoq = GenoQuery(get_repository_path('example01'), tmpdir='/tmp/ukbrest_different/')

        # run
        bgen_file = genoq.get_incl_range(chr=1, start=100, stop=276)

        # validate
        assert bgen_file is not None
        assert isfile(bgen_file)

        results = qctool(bgen_file)

        assert results is not None
        assert hasattr(results, 'shape')
        assert hasattr(results, 'columns')
        assert results.shape[1] == 6 + 300 * 3
        assert results.shape[0] == 3

        assert isdir('/tmp/ukbrest_different/')
        assert len(os.listdir('/tmp/ukbrest_different/')) == 1

# position that does not exist?
# rsids does not exist?

# exclude positions
# exclude rsids
