from os.path import join
from os import devnull
from subprocess import run

from utils.datagen import get_temp_file_name


class UKBQuery:
    def __init__(self, repository_path, debug=False):
        self.repository_path = repository_path
        self.debug = debug

    def _get_chr_file(self, chr):
        return join(self.repository_path, 'chr{:d}impv1.bgen'.format(chr))

    def get_incl_range(self, chr, start, stop):
        chr_file = self._get_chr_file(chr)

        random_bgen_file = get_temp_file_name('.bgen')
        with open(random_bgen_file, 'br+') as bgen_file, open(devnull, 'w') as devnull_file:
            if not self.debug:
                stderr_file = devnull_file
            else:
                stderr_file = None

            run(['bgenix', '-g', chr_file, '-incl-range', '{:02d}:{:d}-{:d}'.format(chr, start, stop)], stdout=bgen_file, stderr=stderr_file)

        return random_bgen_file
