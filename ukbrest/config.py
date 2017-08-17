from os.path import isdir, isfile
from os import environ
from tempfile import gettempdir

genotype_path = environ.get('UKBREST_GENOTYPE_PATH', None)

phenotype_csv = environ.get('UKBREST_PHENOTYPE_CSV', None)
if phenotype_csv:
    phenotype_csv = phenotype_csv.split(';')

# SQL queries will be read by chunks with this size (number of rows)
phenotype_chunksize = environ.get('UKBREST_PHENOTYPE_CHUNKSIZE', None)

db_uri = environ.get('UKBREST_DB_URI', None)
n_columns_per_table = environ.get('UKBREST_N_COLUMNS_PER_TABLE', 1500)
tmp_dir = environ.get('UKBREST_TEMP_DIR', gettempdir())
debug = bool(environ.get('UKBREST_DEBUG', False))
