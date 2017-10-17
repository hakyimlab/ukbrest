import argparse

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.postloader import Postloader
from ukbrest import config

parser = argparse.ArgumentParser()
parser.add_argument('--load-codings', action='store_true')
parser.add_argument('--load-samples-data', action='store_true')
parser.add_argument('--identifier-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...', default=[])
parser.add_argument('--skip-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...', default=[])
parser.add_argument('--separators', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...', default=[])


if __name__ == '__main__':
    args, unknown_args = parser.parse_known_args()

    if args.load_codings:
        pl = Postloader(**config.get_postloader_parameters())
        pl.load_codings(**config.get_postloader_codings_parameters())
    elif args.load_samples_data:
        pl = Postloader(**config.get_postloader_parameters())

        load_samples_parameters = config.get_postloader_samples_data_parameters()
        identifier_columns = {p.split(':')[0]: p.split(':')[1] for p in args.identifier_columns}
        skip_columns = {p.split(':')[0]: p.split(':')[1] for p in args.skip_columns}
        separators = {p.split(':')[0]: p.split(':')[1] for p in args.separators}

        load_samples_parameters.update(identifier_columns)
        load_samples_parameters.update(skip_columns)
        load_samples_parameters.update(separators)

        pl.load_samples_data(**load_samples_parameters)
    else:
        p2sql = Pheno2SQL(**config.get_pheno2sql_parameters())
        p2sql.load_data(**config.get_pheno2sql_load_parameters())
