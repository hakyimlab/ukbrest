import tempfile
import numpy as np
import pandas as pd


def get_temp_file_name(file_extension=''):
    if not file_extension.startswith('.'):
        file_extension = '.' + file_extension

    with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmpfile:
        temp_file_name = tmpfile.name
    return temp_file_name


def generate_random_gen(n_variants, n_samples, chromosome=1, initial_position=100):
    # generate columns
    initial_cols = ['chr', 'snpid', 'rsid', 'pos', 'allele1', 'allele2']

    samples_cols = [['{:d}.aa'.format(i), '{:d}.ab'.format(i), '{:d}.bb'.format(i)] for i in range(1, n_samples + 1)]
    samples_cols = [item for sublist in samples_cols for item in sublist] # flatten

    new_cols = initial_cols + samples_cols

    # start snp id
    initial_snp_id = chromosome * 1000000
    if chromosome == 1:
        initial_snp_id = 1

    # generate variants for each sample
    genotype = []

    current_position = initial_position

    for variant_id in range(n_variants):
        nucleotids = list(np.random.choice(['A', 'G', 'T', 'C'], size=2, replace=False))

        header = [
            '{:02d}'.format(chromosome),
            '{:02d}:{:d}_{}_{}'.format(chromosome, current_position, nucleotids[0], nucleotids[1]),
            'rs{:d}'.format(initial_snp_id + variant_id),
            current_position,
            nucleotids[0],
            nucleotids[1]
        ]

        samples = [
            list(np.random.dirichlet(np.ones(3) + np.random.choice([0, 0, 10], size=3, replace=False), size=1)[0])
            for i in range(n_samples)
        ]
        # BGEN v1.1: accurate to four decimal places; here I round to 5 places, but when read back only 4 should
        # be considered.
        samples = ['{:.5f}'.format(item) for sublist in samples for item in sublist] # flatten

        genotype.append(header + samples)

        current_position += np.random.randint(50, 100)

    return pd.DataFrame(genotype, columns=new_cols)


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('output_file', type=str, help='Output file')
    parser.add_argument('chr', type=int, default=1, help='Chromosome number')
    parser.add_argument('n_variants', type=int, help='Number of variants')
    parser.add_argument('n_samples', type=int, help='Number of samples')
    parser.add_argument('--sample', dest='sample', action='store_true')

    args = parser.parse_args()

    filename, file_extension = os.path.splitext(args.output_file)

    gen_data = generate_random_gen(args.n_variants, args.n_samples, args.chr)
    gen_data.to_csv(args.output_file, sep=' ', header=None, index=False)

    if args.sample:
        samples_ids = list(range(0, args.n_samples + 1))
        samples_data = pd.DataFrame({'ID_1': samples_ids, 'ID_2': samples_ids, 'missing': np.zeros(len(samples_ids), dtype=int)})
        samples_data.to_csv(filename + '.sample', sep=' ', header=True, index=False)
