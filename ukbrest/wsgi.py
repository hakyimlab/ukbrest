from ukbrest import config
from ukbrest.app import app
from ukbrest.common.genoquery import GenoQuery
from ukbrest.common.pheno2sql import Pheno2SQL


def setup_app(app):
    # Add GenoQuery object
    genoq = GenoQuery(config.genotype_path, tmpdir=config.tmpdir, debug=config.debug)
    app.config.update({'genoquery': genoq})

    # Add Pheno2SQL object
    p2sql = Pheno2SQL(**config.get_pheno2sql_parameters())
    app.config.update({'pheno2sql': p2sql})


setup_app(app)
