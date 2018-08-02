import os
import argparse

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.postloader import Postloader
from ukbrest import config
from ukbrest.common.utils.misc import update_parameters_from_args, parameter_empty

from ukbrest.resources.error_handling import handle_errors

parser = argparse.ArgumentParser()
parser.add_argument('--load-codings', action='store_true')
parser.add_argument('--load-samples-data', action='store_true')
parser.add_argument('--identifier-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')
parser.add_argument('--skip-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')
parser.add_argument('--separators', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')


@handle_errors
def load_codings(args):
    pl = Postloader(**config.get_postloader_parameters())
    pl.load_codings(**config.get_postloader_codings_parameters())


@handle_errors
def load_samples_data(args):
    pl = Postloader(**config.get_postloader_parameters())

    load_samples_parameters = config.get_postloader_samples_data_parameters()

    if args.identifier_columns is not None:
        load_samples_parameters.update(
            {'identifier_columns': {p.split(':')[0]: p.split(':')[1].split(',') for p in args.identifier_columns}})

    if args.skip_columns is not None:
        load_samples_parameters.update(
            {'skip_columns': {p.split(':')[0]: p.split(':')[1].split(',') for p in args.skip_columns}})

    if args.separators is not None:
        load_samples_parameters.update({'separators': {p.split(':')[0]: p.split(':')[1] for p in args.separators}})

    pl.load_samples_data(**load_samples_parameters)


@handle_errors
def load_data(args):
    pheno2sql_parameters = config.get_pheno2sql_parameters()
    pheno2sql_parameters = update_parameters_from_args(pheno2sql_parameters, args)

    # FIXME: parameter names hard coded here
    if parameter_empty(pheno2sql_parameters, 'ukb_csvs'):
        if args.pheno_dir is None:
            parser.error('--pheno-dir missing')

        pheno2sql_parameters['ukb_csvs'] = sorted([
            os.path.join(args.pheno_dir, f)
            for f in os.listdir(args.pheno_dir)
            if f.lower().endswith('.csv')
        ])

    if parameter_empty(pheno2sql_parameters, 'db_uri'):
        parser.error('--db-uri missing')

    p2sql = Pheno2SQL(**pheno2sql_parameters)

    load_parameters = config.get_pheno2sql_load_parameters()

    p2sql.load_data(**load_parameters)


if __name__ == '__main__':
    parser = config.get_argparse_arguments(parser)
    args, unknown_args = parser.parse_known_args()

    if args.load_codings:
        load_codings(args)

    elif args.load_samples_data:
        load_samples_data(args)

    else:
        load_data(args)
