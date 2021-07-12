from os import environ, path
from tempfile import gettempdir
import logging


#############################
# Environment variables names
#############################
GENOTYPE_BGENIX_PATH='UKBREST_GENOTYPE_BGENIX_PATH'
GENOTYPE_PATH_ENV='UKBREST_GENOTYPE_PATH'
GENOTYPE_BGEN_FILE_NAMING='UKBREST_GENOTYPE_BGEN_FILE_NAMING'
GENOTYPE_BGEN_SAMPLE='UKBREST_GENOTYPE_BGEN_SAMPLE_FILE'

PHENOTYPE_PATH='UKBREST_PHENOTYPE_PATH'
PHENOTYPE_CSV_ENV='UKBREST_PHENOTYPE_CSV'
CODINGS_PATH='UKBREST_CODINGS_PATH'
WITHDRAWALS_PATH='UKBREST_WITHDRAWALS_PATH'
SAMPLES_DATA_PATH='UKBREST_SAMPLES_DATA_PATH'

TABLE_PREFIX_ENV='UKBREST_TABLE_PREFIX'
LOADING_CHUNKSIZE='UKBREST_LOADING_CHUNKSIZE'
DB_URI_ENV='UKBREST_DB_URI'
N_COLUMNS_PER_TABLE_ENV='UKBREST_N_COLUMNS_PER_TABLE'
TMP_DIR_ENV='UKBREST_TEMP_DIR'
DEBUG_ENV='UKBREST_DEBUG'
SQL_CHUNKSIZE_ENV='UKBREST_SQL_CHUNKSIZE'
LOADING_N_JOBS_ENV= 'UKBREST_LOADING_N_JOBS'

LOAD_DATA_VACUUM = 'UKBREST_VACUUM'

HTTP_AUTH_USERS_FILE = 'UKBREST_HTTP_USERS_FILE_PATH'

PRIMARY_CARE_DIR = 'PRIMARY_CARE_DIR'
HOSPITAL_INPATIENT_DIR = 'HOSPITAL_INPATIENT_DIR'


########################
# Configuration defaults
########################
genotype_bgenix_path = environ.get(GENOTYPE_BGENIX_PATH, 'bgenix')

genotype_path = environ.get(GENOTYPE_PATH_ENV, None)

genotype_bgen_file_naming = environ.get(GENOTYPE_BGEN_FILE_NAMING, 'chr{:d}impv1.bgen')

# bgen sample file is relative to genotype path
bgen_sample_file = environ.get(GENOTYPE_BGEN_SAMPLE, None)
if bgen_sample_file is not None:
    bgen_sample_file = path.join(genotype_path, bgen_sample_file)

phenotype_path = environ.get(PHENOTYPE_PATH, None)
phenotype_csv = environ.get(PHENOTYPE_CSV_ENV, None)
if phenotype_csv is not None:
    phenotype_csv = phenotype_csv.split(';')

codings_path = environ.get(CODINGS_PATH, None)
withdrawals_path = environ.get(WITHDRAWALS_PATH, None)

samples_data_path = environ.get(SAMPLES_DATA_PATH, None)
if samples_data_path is not None:
    samples_data_path = path.join(phenotype_path, samples_data_path)

db_uri = environ.get(DB_URI_ENV, None)
table_prefix = environ.get(TABLE_PREFIX_ENV, 'ukb_pheno_')
n_columns_per_table = environ.get(N_COLUMNS_PER_TABLE_ENV, 1500)
tmpdir = environ.get(TMP_DIR_ENV, gettempdir())
debug = bool(environ.get(DEBUG_ENV, False))

# SQL queries will be read by chunks with this size (number of rows)
sql_chunksize = environ.get(SQL_CHUNKSIZE_ENV, None)

loading_chunksize = environ.get(LOADING_CHUNKSIZE, 5000)

loading_n_jobs = environ.get(LOADING_N_JOBS_ENV, -1)

load_data_vacuum = environ.get(LOAD_DATA_VACUUM, True)

http_auth_users_file = environ.get(HTTP_AUTH_USERS_FILE, None)

# EHR loading parameters
primary_care_dir = environ.get(PRIMARY_CARE_DIR, None)
hospital_inpatient_dir = environ.get(HOSPITAL_INPATIENT_DIR, None)


########
# logger
########
FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO if not debug else logging.DEBUG)
logger = logging.getLogger('ukbrest')


###########
# Functions
###########
def get_postloader_parameters():
    return {
        'db_uri': db_uri,
    }


def get_postloader_codings_parameters():
    return {
        'codings_dir': codings_path,
    }


def get_postloader_withdrawals_parameters():
    return {
        'withdrawals_dir': withdrawals_path,
    }


def get_postloader_samples_data_parameters():
    return {
        'data_dir': samples_data_path,
    }


def get_genoquery_parameters():
    return {
        'genotype_path': genotype_path,
        'bgen_names': genotype_bgen_file_naming,
        'bgenix_path': genotype_bgenix_path,
        'tmpdir': tmpdir,
        'debug': debug,
    }


def get_pheno2sql_parameters():
    return {
        'ukb_csvs': phenotype_csv,
        'bgen_sample_file': bgen_sample_file,
        'db_uri': db_uri,
        'table_prefix': table_prefix,
        'n_columns_per_table': int(n_columns_per_table),
        'loading_n_jobs': int(loading_n_jobs),
        'tmpdir': tmpdir,
        'loading_chunksize': int(loading_chunksize),
        'sql_chunksize': int(sql_chunksize) if sql_chunksize is not None else None,
    }


def get_pheno2sql_load_parameters():
    return {
        'vacuum': load_data_vacuum
    }

def get_ehr2sql_parameters():
    return {
        'primary_care_dir': primary_care_dir,
        'hospital_inpatient_dir': hospital_inpatient_dir,
        'db_uri': db_uri,
#        'table_prefix': table_prefix,
        'n_columns_per_table': int(n_columns_per_table),
        'loading_n_jobs': int(loading_n_jobs),
        'tmpdir': tmpdir,
        'loading_chunksize': int(loading_chunksize),
        'sql_chunksize': int(sql_chunksize) if sql_chunksize is not None else None,
    }

def get_pheno_query_parameters():
    return {
        'db_uri': db_uri,
        'sql_chunksize': int(sql_chunksize) if sql_chunksize is not None else None,
    }

def get_ehr_query_parameters():
    return {
        'db_uri': db_uri,
        'sql_chunksize': int(sql_chunksize) if sql_chunksize is not None else None,
    }

def get_argparse_arguments(parser=None):
    import argparse

    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument('--genotype-path', type=str, help='Genotypes path, where BGEN files reside')
    parser.add_argument('--bgen-names', type=str, help='BGEN file name format')
    parser.add_argument('--pheno-dir', type=str, help='Phenotypes directory, where csv files reside.')
    parser.add_argument('--withdrawals-dir', type=str, help='Directory with withdrawals csv files.')
    parser.add_argument('--codings-dir', type=str, help='Directory with codings tsv files')
    parser.add_argument('--bgen-sample-file', type=str, help='BGEN sample file')
    parser.add_argument('--db-uri', type=str, help='PostgreSQL connection string in the format: postgresql://USER:PASSWORD@HOST:5432/DB_NAME')
    parser.add_argument('--table-prefix', type=str, help='This is used when creating the database tables and loading data')
    parser.add_argument('--n-columns-per-table', type=int, help='How many columns per table you want to store. PostgreSQL has a limit; we use 1500 by default')
    parser.add_argument('--loading-n-jobs', type=int, help='The loading step will be parallelized with n number of jobs. By default it is the number of cores')
    parser.add_argument('--tmpdir', type=str, help='Temporal directory. Temporary CSV files are written here.')
    parser.add_argument('--loading-chunksize', type=int, help='For the loading step, this will specify the number of rows read each time from CSV files. It is set to 5000 by default.')
    parser.add_argument('--sql-chunksize', type=int, help='When performing any SQL query, this will be the the number of rows processed at each time. 5000 rows by default.')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--host', type=str, help='Host', default='127.0.0.1')
    parser.add_argument('--port', type=int, help='Port where to listen to')
    parser.add_argument('--users-file', type=str, help='This users files, if not empty, activates HTTP Basic Authentication. It must be a valid YAML file: one line per user with format USER: PASSWORD')
    parser.add_argument('--ssl-mode', action='store_true', help='Activates SSL in adhoc mode when running with Flask.')
    parser.add_argument('--primary-care-dir', type=str, help="Directory with primary care EHR files: gp_clinical.txt, gp_registrations.txt, gp_scripts.txt")
    parser.add_argument('--hospital-inpatient-dir', type=str, help= "Directory with hospital inpatient EHR files: hesin.txt, hesin_diag.txt")

    return parser
