from ukbrest import config
from ukbrest.app import app
from ukbrest.common.genoquery import GenoQuery
from ukbrest.common.pheno2sql import Pheno2SQL


def setup_app(app):
    # Add GenoQuery object
    genoq = GenoQuery(config.genotype_path, tmpdir=config.tmp_dir, debug=config.debug)
    app.config.update({'genoquery': genoq})

    # Add Pheno2SQL object
    p2sql = Pheno2SQL(config.phenotype_csv, config.db_uri, n_columns_per_table=config.n_columns_per_table,
                      tmpdir=config.tmp_dir, sql_chunksize=config.phenotype_chunksize)
    app.config.update({'pheno2sql': p2sql})


setup_app(app)
