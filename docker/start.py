#!/usr/bin/env python

from os import listdir, execvp
from os import environ
from os.path import isdir, join
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--csv_filename', dest='csv_filename', required=False, type=str, help='Phenotype CSV file name', default=None)
parser.add_argument('--start', dest='start', action='store_true', required=False, help='Specifies whether the HTTP server should be started.', default=True)
parser.add_argument('--load', dest='load', action='store_true', required=False, help='Specifies whether data should be loaded into the DB.', default=False)

args, unknown = parser.parse_known_args()

### Check if genotype dir has .bgen files
genotype_path = environ.get('UKBREST_GENOTYPE_PATH', None)
phenotype_path = environ.get('UKBREST_PHENOTYPE_PATH', None)
db_uri = environ.get('UKBREST_DB_URI', None)

if not isdir(genotype_path):
    parser.error('The genotype directory does not exist. You have to mount it using '
                 'the option "-v hostDir:{}" of "docker run"'.format(genotype_path))

bgen_files = [f for f in listdir(genotype_path) if f.lower().endswith('.bgen')]
if len(bgen_files) == 0:
    parser.error('No .bgen files were found in the genotype directory')


### Check if phenotype dir has the .csv and .html files needed
if not isdir(phenotype_path):
    parser.error('The phenotype directory does not exist. You have to mount it using '
                 'the option "-v hostDir:{}" of "docker run"'.format(phenotype_path))

# check whether there is at least one and only one csv file
phenotype_csv_file = [f for f in listdir(phenotype_path) if f.lower().endswith('.csv')]

if len(phenotype_csv_file) == 0:
    parser.error('No .csv files were found in the phenotype directory')

if len(phenotype_csv_file) > 1 and not args.csv_filename:
    parser.error('More than one .csv files were found in the phenotype directory. '
                 'Specify which one should be used with option --csv_filename')
elif len(phenotype_csv_file) > 1 and args.csv_filename:
    phenotype_csv_filename = args.csv_filename
else:
    phenotype_csv_filename = phenotype_csv_file[0]

environ['UKBREST_PHENOTYPE_CSV'] = join(phenotype_path, phenotype_csv_filename)


### Check if DB URI was specified
if db_uri is None:
    parser.error('No DB URI was specified. You have to set it using the environment variable UKBREST_DB_URI. For '
                 'example, for PostgreSQL, the format is: postgresql://user:pass@host:port/dbname')

if args.load:
    commands = ('python', ['python', '/opt/ukbrest/load_data.py'])
elif args.start:
    commands = ('gunicorn', ['gunicorn', 'ukbrest.wsgi:app'])

execvp(*commands)
