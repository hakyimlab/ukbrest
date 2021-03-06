from ukbrest import config
from ukbrest.app import app
from ukbrest.common.genoquery import GenoQuery
from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.utils.auth import PasswordHasher


def setup_app(app, ph):
    # Add GenoQuery object
    genoq = GenoQuery(**config.get_genoquery_parameters())
    app.config.update({'genoquery': genoq})

    # Add Pheno2SQL object
    p2sql = Pheno2SQL(**config.get_pheno2sql_parameters())
    app.config.update({'pheno2sql': p2sql})

    # Add auth object
    auth = ph.setup_http_basic_auth()
    app.config.update({'auth': auth})


ph = PasswordHasher(config.http_auth_users_file, method='pbkdf2:sha256')
ph.process_users_file()

setup_app(app, ph)
