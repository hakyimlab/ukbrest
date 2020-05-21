import os
import argparse

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.ehr2sql import EHR2SQL
from ukbrest.common.postloader import Postloader
from ukbrest import config
from ukbrest.common.utils.misc import update_parameters_from_args, parameter_empty

from ukbrest.resources.error_handling import handle_errors

parser = argparse.ArgumentParser("Loads data into the SQL database")
parser.add_argument('--load-sql', action='store_true')
parser.add_argument('--load-withdrawals', action='store_true', help="Loads withdrawals and then exits. Should be used with --withdrawals-dir")
parser.add_argument('--load-codings', action='store_true', help="Loads codings and then exits. Should be used with --codings-dir.")
parser.add_argument('--load-samples-data', action='store_true')
parser.add_argument('--load-ehr', action='store_true', help="Load record-level EHR data")
parser.add_argument('--identifier-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')
parser.add_argument('--skip-columns', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')
parser.add_argument('--separators', type=str, nargs='+', help='Format file1.txt:column1 file2.txt:column2 ...')


@handle_errors
def load_withdrawals(args):
    pl_params = config.get_postloader_parameters()
    pl_params = update_parameters_from_args(pl_params, args)
    pl = Postloader(**pl_params)

    pl_wd_params = config.get_postloader_withdrawals_parameters()
    pl_wd_params = update_parameters_from_args(pl_wd_params, args)
    pl.load_withdrawals(**pl_wd_params)


@handle_errors
def load_codings(args):
    pl_params = config.get_postloader_parameters()
    pl_params = update_parameters_from_args(pl_params, args)
    pl = Postloader(**pl_params)

    pl_coding_params = config.get_postloader_codings_parameters()
    pl_coding_params = update_parameters_from_args(pl_coding_params, args)
    pl.load_codings(**pl_coding_params)


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


@handle_errors
def load_sql():
    pheno2sql_parameters = config.get_pheno2sql_parameters()
    pheno2sql_parameters = update_parameters_from_args(pheno2sql_parameters, args)

    if parameter_empty(pheno2sql_parameters, 'db_uri'):
        parser.error('--db-uri missing')

    p2sql = Pheno2SQL(**pheno2sql_parameters)

    p2sql.load_sql('/opt/utils/sql/functions.sql')

@handle_errors
def load_ehr(args):
    # pheno2sql_parameters = config.get_pheno2sql_parameters()
    # pheno2sql_parameters = update_parameters_from_args(pheno2sql_parameters, args)

    ehr2sql_parameters = config.get_ehr2sql_parameters()
    ehr2sql_parameters = update_parameters_from_args(ehr2sql_parameters, args)

    ehr2sql = EHR2SQL(**ehr2sql_parameters)

    load_parameters = config.get_pheno2sql_load_parameters()

    ehr2sql.load_data(**load_parameters)


if __name__ == '__main__':
    parser = config.get_argparse_arguments(parser)
    args, unknown_args = parser.parse_known_args()

    if args.load_codings:
        load_codings(args)

    elif args.load_withdrawals:
        load_withdrawals(args)

    elif args.load_samples_data:
        load_samples_data(args)

    elif args.load_ehr:
        load_ehr(args)

    elif args.load_sql:
        load_sql()

    else:
        load_data(args)
