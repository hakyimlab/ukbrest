from os import environ
from tempfile import gettempdir
import logging

GENOTYPE_PATH_ENV='UKBREST_GENOTYPE_PATH'
PHENOTYPE_PATH='UKBREST_PHENOTYPE_PATH'
PHENOTYPE_CSV_ENV='UKBREST_PHENOTYPE_CSV'
TABLE_PREFIX_ENV='UKBREST_TABLE_PREFIX'
LOADING_CHUNKSIZE='UKBREST_LOADING_CHUNKSIZE'
DB_URI_ENV='UKBREST_DB_URI'
N_COLUMNS_PER_TABLE_ENV='UKBREST_N_COLUMNS_PER_TABLE'
TMP_DIR_ENV='UKBREST_TEMP_DIR'
DEBUG_ENV='UKBREST_DEBUG'
SQL_CHUNKSIZE_ENV='UKBREST_SQL_CHUNKSIZE'
LOADING_N_JOBS_ENV= 'UKBREST_LOADING_N_JOBS'


genotype_path = environ.get(GENOTYPE_PATH_ENV, None)

phenotype_csv = environ.get(PHENOTYPE_CSV_ENV, None)
if phenotype_csv is not None:
    phenotype_csv = phenotype_csv.split(';')

db_uri = environ.get(DB_URI_ENV, None)
table_prefix = environ.get(TABLE_PREFIX_ENV, 'ukb_pheno_')
n_columns_per_table = environ.get(N_COLUMNS_PER_TABLE_ENV, 1500)
tmpdir = environ.get(TMP_DIR_ENV, gettempdir())
debug = bool(environ.get(DEBUG_ENV, False))

# SQL queries will be read by chunks with this size (number of rows)
sql_chunksize = environ.get(SQL_CHUNKSIZE_ENV, None)

loading_chunksize = environ.get(LOADING_CHUNKSIZE, 5000)

loading_n_jobs = environ.get(LOADING_N_JOBS_ENV, -1)

# logger
logger = logging.getLogger('ukbrest')
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

def get_pheno2sql_parameters():
    return {
        'ukb_csvs': phenotype_csv,
        'db_uri': db_uri,
        'table_prefix': table_prefix,
        'n_columns_per_table': int(n_columns_per_table),
        'loading_n_jobs': int(loading_n_jobs),
        'tmpdir': tmpdir,
        'loading_chunksize': int(loading_chunksize),
        'sql_chunksize': int(sql_chunksize) if sql_chunksize is not None else None,
    }
