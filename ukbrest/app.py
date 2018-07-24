import logging

from flask import Flask
from ukbrest.resources.phenotype import PhenotypeFieldsAPI, PhenotypeAPI, QueryAPI, PhenotypeApiObject

from ukbrest.resources.genotype import GenotypeApiObject
from ukbrest.resources.genotype import GenotypePositionsAPI, GenotypeRsidsAPI


app = Flask(__name__)


# Genotype API
genotype_api = GenotypeApiObject(app)

genotype_api.add_resource(
    GenotypePositionsAPI,
    '/ukbrest/api/v1.0/genotype/<int:chr>/positions',
    '/ukbrest/api/v1.0/genotype/<int:chr>/positions/<int:start>',
    '/ukbrest/api/v1.0/genotype/<int:chr>/positions/<int:start>/<int:stop>',
)

genotype_api.add_resource(
    GenotypeRsidsAPI,
    '/ukbrest/api/v1.0/genotype/<int:chr>/rsids',
)

# Phenotype API
phenotype_api = PhenotypeApiObject(app)

phenotype_api.add_resource(
    PhenotypeAPI,
    '/ukbrest/api/v1.0/phenotype',
)

# Phenotype Info API
phenotype_info_api = PhenotypeApiObject(app, default_mediatype='application/json')

phenotype_info_api.add_resource(
    PhenotypeFieldsAPI,
    '/ukbrest/api/v1.0/phenotype/fields',
)

# Query API
phenotype_api = PhenotypeApiObject(app)

phenotype_api.add_resource(
    QueryAPI,
    '/ukbrest/api/v1.0/query',
)

@app.before_first_request
def setup_logging():
    if not app.debug:
        # In production mode, add log handler to sys.stderr.
        app.logger.addHandler(logging.StreamHandler())
        app.logger.setLevel(logging.INFO)


if __name__ == '__main__':
    from ukbrest.common.genoquery import GenoQuery
    from ukbrest.common.pheno2sql import Pheno2SQL
    from ukbrest.common.utils.auth import PasswordHasher
    from ukbrest import config
    from ukbrest.common.utils.misc import _update_parameters_from_args, _parameter_empty

    logger = config.logger
    parser = config.get_argparse_arguments()

    args = parser.parse_args()

    # GenoQuery
    genoq_parameters = config.get_genoquery_parameters()
    genoq_parameters = _update_parameters_from_args(genoq_parameters, args)

    if _parameter_empty(genoq_parameters, 'genotype_path'):
        logger.warning('--genotype-path missing')

    genoq = GenoQuery(**genoq_parameters)
    app.config.update({'genoquery': genoq})

    # Pheno2SQL
    pheno2sql_parameters = config.get_pheno2sql_parameters()
    pheno2sql_parameters = _update_parameters_from_args(pheno2sql_parameters, args)

    if _parameter_empty(pheno2sql_parameters, 'db_uri'):
        parser.error('--db-uri missing')

    p2sql = Pheno2SQL(**pheno2sql_parameters)

    app.config.update({'pheno2sql': p2sql})

    ph = PasswordHasher(args.users_file, method='pbkdf2:sha256')
    ph.process_users_file()
    auth = ph.setup_http_basic_auth()
    app.config.update({'auth': auth})

    app.run(host=str(args.host), port=args.port, debug=args.debug, ssl_context='adhoc' if args.ssl_mode else None)
