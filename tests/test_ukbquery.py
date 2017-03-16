from os.path import join, dirname, abspath
import unittest

from common.ukbquery import UKBQuery


def get_repository_path(data_filename):
    directory = dirname(abspath(__file__))
    directory = join(directory, 'data/')
    return join(directory, data_filename)


class UKBQueryTest(unittest.TestCase):
    def test_query_incl_range_lower_and_upper_limits(self):
        # prepare
        ukbq = UKBQuery(get_repository_path('example01'))

        # run
        results = ukbq.get_incl_range(chr=1, start=100, stop=290)

        # validate
        assert results is not None
        assert hasattr(results, 'shape')

    def test_getbgen_incl_range_lower_and_upper_limits(self):
        super(UKBQueryTest, self).fail('Not implemented')
