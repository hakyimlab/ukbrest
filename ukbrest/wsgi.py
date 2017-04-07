from os.path import isdir

from ukbrest.app import app
from ukbrest import settings
from ukbrest.common.genoquery import GenoQuery
from ukbrest.common.pheno2sql import Pheno2SQL

def setup_app(app):
    if not isdir(settings.genotype_path):
        raise Exception('Repository path does not exist: {}'.format(settings.genotype_path))

    app.config.update({'genoquery': GenoQuery(settings.genotype_path, debug=settings.debug)})

    csv_file = settings.phenotype_csv
    db_engine = settings.db_uri
    p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=1500, tmpdir=settings.tmp_dir)
    if settings.load_db:
        p2sql.load_data()

    app.config.update({'pheno2sql': p2sql})

#    app.run(host=str(settings.host), port=settings.port, debug=settings.debug)

setup_app(app)

