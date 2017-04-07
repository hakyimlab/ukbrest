from os import devnull
from os.path import join
from subprocess import run

from common.utils.datagen import get_temp_file_name


class GenoQuery:
    def __init__(self, repository_path, debug=False):
        self.repository_path = repository_path
        self.debug = debug

    def _get_chr_file(self, chr):
        return join(self.repository_path, 'chr{:d}impv1.bgen'.format(chr))

    def get_incl_range(self, chr, start=None, stop=None):
        chr_file = self._get_chr_file(chr)

        random_bgen_file = get_temp_file_name('.bgen')
        with open(random_bgen_file, 'br+') as bgen_file, open(devnull, 'w') as devnull_file:
            stderr_file = None if self.debug else devnull_file
            run(['bgenix', '-g', chr_file, '-incl-range', '{:02d}:{}-{}'.format(chr, start or '', stop or '')], stdout=bgen_file, stderr=stderr_file)

        return random_bgen_file

    def get_incl_range_from_file(self, chr, filepath):
        chr_file = self._get_chr_file(chr)

        random_bgen_file = get_temp_file_name('.bgen')
        with open(random_bgen_file, 'br+') as bgen_file, open(devnull, 'w') as devnull_file:
            stderr_file = None if self.debug else devnull_file
            run(['bgenix', '-g', chr_file, '-incl-range', '{}'.format(filepath)], stdout=bgen_file, stderr=stderr_file)

        return random_bgen_file

    def get_incl_rsids(self, chr, rsids):
        chr_file = self._get_chr_file(chr)

        random_bgen_file = get_temp_file_name('.bgen')
        with open(random_bgen_file, 'br+') as bgen_file, open(devnull, 'w') as devnull_file:
            stderr_file = None if self.debug else devnull_file
            bgen_command = ['bgenix', '-g', chr_file, '-incl-rsids'] + rsids
            run(bgen_command, stdout=bgen_file, stderr=stderr_file)

        return random_bgen_file
