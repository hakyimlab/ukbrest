#!/usr/bin/env python

from os import listdir, execvp
from os import environ
from os.path import isdir, join
import argparse

from ukbrest.config import logger, GENOTYPE_PATH_ENV, PHENOTYPE_PATH, PHENOTYPE_CSV_ENV, DB_URI_ENV, CODINGS_PATH, \
    SAMPLES_DATA_PATH, WITHDRAWALS_PATH

parser = argparse.ArgumentParser()
parser.add_argument('--load', action='store_true', help='Specifies whether data should be loaded into the DB.')
parser.add_argument('--load-sql', action='store_true', help='Loads some useful SQL functions into the database.')
parser.add_argument('--load-codings', action='store_true', help='Loads a set of codings files (coding_NUM.tsv).')
parser.add_argument('--load-withdrawals', action='store_true', help='Loads a list of participants who has withdrawn consent (*.csv files).')
parser.add_argument('--load-samples-data', action='store_true', help='Loads a set of files containing information about samples.')

args, unknown_args = parser.parse_known_args()


def _setup_genotype_path():
    genotype_path = environ.get(GENOTYPE_PATH_ENV, None)

    if not isdir(genotype_path):
        logger.warning('The genotype directory does not exist. You have to mount it using '
                       'the option "-v hostDir:{}" of "docker run"'.format(genotype_path))
        return

    bgen_files = [f for f in listdir(genotype_path) if f.lower().endswith('.bgen')]
    if len(bgen_files) == 0:
        logger.warning('No .bgen files were found in the genotype directory')

    bgi_files = [f for f in listdir(genotype_path) if f.lower().endswith('.bgi')]
    if len(bgi_files) == 0:
        logger.warning('No .bgi files were found in the genotype directory')


def _setup_phenotype_path():
    phenotype_path = environ.get(PHENOTYPE_PATH, None)

    if not isdir(phenotype_path):
        parser.error('The phenotype directory does not exist. You have to mount it using '
                     'the option "-v hostDir:{}" of "docker run"'.format(phenotype_path))

    # check whether there is at least one and only one csv file
    phenotype_csv_file = sorted([f for f in listdir(phenotype_path) if f.lower().endswith('.csv')])

    if len(phenotype_csv_file) == 0:
        parser.error('No .csv files were found in the phenotype directory')

    environ[PHENOTYPE_CSV_ENV] = ';'.join([join(phenotype_path, csv_file) for csv_file in phenotype_csv_file])


def _setup_codings():
    phenotype_path = environ.get(PHENOTYPE_PATH, None)
    coding_path = environ.get(CODINGS_PATH, None)

    if coding_path is None:
        environ[CODINGS_PATH] = 'codings'
        coding_path = 'codings'

    coding_path = join(phenotype_path, coding_path)

    if not isdir(coding_path):
        parser.error('The codings directory does not exist: {}'.format(coding_path))


def _setup_withdrawals():
    withdrawals_path = environ.get(WITHDRAWALS_PATH, None)

    if withdrawals_path is None:
        parser.error('The withdrawals directory was not specified')

    if not isdir(withdrawals_path):
        parser.error('The withdrawals directory does not exist: {}'.format(withdrawals_path))


def _setup_samples_data():
    phenotype_path = environ.get(PHENOTYPE_PATH, None)
    samples_data_path = environ.get(SAMPLES_DATA_PATH, None)

    if samples_data_path is None:
        environ[SAMPLES_DATA_PATH] = 'samples_data'
        samples_data_path = 'samples_data'

    samples_data_path = join(phenotype_path, samples_data_path)

    if not isdir(samples_data_path):
        parser.error('The samples data directory does not exist: {}'.format(samples_data_path))


def _setup_db_uri():
    db_uri = environ.get(DB_URI_ENV, None)

    if db_uri is None:
        parser.error('No DB URI was specified. You have to set it using the environment variable UKBREST_DB_URI. For '
                     'example, for PostgreSQL, the format is: postgresql://user:pass@host:port/dbname')


if __name__ == '__main__':
    if args.load:
        _setup_phenotype_path()
        _setup_db_uri()

        commands = ('python', ['python', '/opt/ukbrest/load_data.py'])

    elif args.load_sql:
        _setup_db_uri()

        commands = ('python', ['python', '/opt/ukbrest/load_data.py', '--load-sql'])

    elif args.load_codings:
        _setup_codings()
        _setup_db_uri()

        commands = ('python', ['python', '/opt/ukbrest/load_data.py', '--load-codings'])

    elif args.load_withdrawals:
        _setup_withdrawals()
        _setup_db_uri()

        commands = ('python', ['python', '/opt/ukbrest/load_data.py', '--load-withdrawals'])

    elif args.load_samples_data:
        _setup_samples_data()
        _setup_db_uri()

        commands = ('python', ['python', '/opt/ukbrest/load_data.py', '--load-samples-data'] + unknown_args)

    else:
        _setup_genotype_path()
        _setup_db_uri()
        # TODO: check if data was loaded into PostgreSQL

        commands = ('gunicorn', ['gunicorn', 'ukbrest.wsgi:app'])

    execvp(*commands)
