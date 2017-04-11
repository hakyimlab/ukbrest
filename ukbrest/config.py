from os.path import isdir, isfile
from os import environ
from tempfile import gettempdir

genotype_path = environ.get('UKBREST_GENOTYPE_PATH', None)
phenotype_csv = environ.get('UKBREST_PHENOTYPE_CSV', None)
db_uri = environ.get('UKBREST_DB_URI', None)
n_columns_per_table = environ.get('UKBREST_N_COLUMNS_PER_TABLE', 1500)
tmp_dir = environ.get('UKBREST_TEMP_DIR', gettempdir())
debug = bool(environ.get('UKBREST_DEBUG', False))

# Validate parameters
if genotype_path is None or not isdir(genotype_path):
    raise Exception('Genotype repository path does not exist or not specified: {}'.format(genotype_path))

if phenotype_csv is None or not isfile(phenotype_csv):
    raise Exception('Phenotype csv file (ukbXXXX.csv) does not exist or not specified: {}'.format(phenotype_csv))

if db_uri is None or db_uri.strip() == '':
    raise Exception('DB URI was not specified or is not valid')

if tmp_dir and not isdir(tmp_dir):
    raise Exception('The temporary directory specified does not exist: {}'.format(tmp_dir))
