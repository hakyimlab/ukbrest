from os.path import devnull
from subprocess import call

import pandas as pd

from ukbrest.common.utils.datagen import get_temp_file_name


def qctool(bgen_file, debug=False):
    random_gen_file = get_temp_file_name('.gen')

    with open(devnull, 'w') as devnull_file:
        if not debug:
            stderr_file = devnull_file
        else:
            stderr_file = None

        call(['qctool', '-g', bgen_file, '-og', random_gen_file], stdout=devnull_file, stderr=stderr_file)

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

    return pd.read_table(random_gen_file, sep=' ', header=None, names=new_cols).round(4)
