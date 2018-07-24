import os
import shutil
import subprocess

from ukbrest.common.utils.datagen import get_temp_file_name, get_tmpdir
from ukbrest.config import logger
from ukbrest.resources.exceptions import UkbRestValidationError, UkbRestExecutionError


class GenoQuery:
    def __init__(self, genotype_path, bgen_names='chr{:d}impv1.bgen', bgenix_path='bgenix', tmpdir='/tmp/ukbrest', debug=False):
        self.repository_path = genotype_path
        self.bgen_names = bgen_names
        self.bgenix_path = bgenix_path
        self.tmpdir = tmpdir
        self.debug = debug

    def _get_chr_file(self, chr):
        chr_file = os.path.join(self.repository_path, self.bgen_names.format(chr))

        if not os.path.isfile(chr_file):
            raise UkbRestValidationError(f'BGEN file not found: {chr_file}')

        return chr_file

    def _get_bgenix_path(self):
        if shutil.which(self.bgenix_path) is None:
            raise UkbRestValidationError(f'bgenix was not found: {self.bgenix_path}')

        return self.bgenix_path

    def _run_bgenix(self, arguments):
        random_bgen_file = get_temp_file_name('.bgen', tmpdir=get_tmpdir(self.tmpdir))

        with open(random_bgen_file, 'br+') as bgen_file:
            full_command = [self._get_bgenix_path()] + arguments

            logger.info(f'Running: {full_command}')

            run_status = subprocess.run(
                full_command, stdout=bgen_file, stderr=subprocess.PIPE
            )

            if run_status.returncode != 0:
                raise UkbRestExecutionError(
                    f'bgenix failed: {" ".join(run_status.args)}',
                    run_status.stderr.decode(),
                )

        return random_bgen_file

    def get_incl_range(self, chr, start=None, stop=None):
        chr_file = self._get_chr_file(chr)
        bgenix_args = ['-g', chr_file, '-incl-range', '{:02d}:{}-{}'.format(chr, start or '', stop or '')]

        return self._run_bgenix(bgenix_args)

    def get_incl_range_from_file(self, chr, filepath):
        chr_file = self._get_chr_file(chr)
        bgenix_args = ['-g', chr_file, '-incl-range', '{}'.format(filepath)]

        return self._run_bgenix(bgenix_args)

    def get_incl_rsids(self, chr, rsids):
        chr_file = self._get_chr_file(chr)
        bgenix_args = ['-g', chr_file, '-incl-rsids'] + [rsids]

        return self._run_bgenix(bgenix_args)
