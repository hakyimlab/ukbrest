from os.path import join, dirname, abspath, isfile
from subprocess import call
import unittest
import pandas as pd

from common.ukbquery import UKBQuery
from utils.datagen import get_temp_file_name


def get_repository_path(data_filename):
    directory = dirname(abspath(__file__))
    directory = join(directory, 'data/')
    return join(directory, data_filename)


def qctool(bgen_file):
    random_gen_file = get_temp_file_name('.gen')
    call(['qctool', '-g', bgen_file, '-og', random_gen_file])

    # read how many columns the file has
    with open(random_gen_file, 'r') as f:
        first_line = f.readline()

    initial_cols = ['chr', 'snpid', 'rsid', 'pos', 'allele1', 'allele2']

    n_columns = len(first_line.split(' '))
    n_columns_without_initial_cols = n_columns - len(initial_cols)

    if n_columns_without_initial_cols % 3 == 0:
        n_samples = int(n_columns_without_initial_cols / 3)
    else:
        raise Exception('malformed .gen file')

    samples_cols = [['{:d}.aa'.format(i), '{:d}.ab'.format(i), '{:d}.bb'.format(i)] for i in range(1, n_samples + 1)]
    samples_cols = [item for sublist in samples_cols for item in sublist] # flatten

    new_cols = initial_cols + samples_cols

    return pd.read_table(random_gen_file, sep=' ', header=None, names=new_cols)


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

    def test_getbgen_incl_range_lower_and_upper_limits(self):
        super(UKBQueryTest, self).fail('Not implemented')
