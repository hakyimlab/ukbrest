import logging

from flask import Flask
from flask import jsonify

from ukbrest.resources.phenotype import PhenotypeFieldsAPI, PhenotypeAPI, PhenotypeApiObject
from ukbrest.resources.query import PhenoQueryAPI, EHRQueryAPI, QueryApiObject
from ukbrest.resources.genotype import GenotypeApiObject
from ukbrest.resources.genotype import GenotypePositionsAPI, GenotypeRsidsAPI
from ukbrest.resources.exceptions import UkbRestException

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
query_api = QueryApiObject(app)

query_api.add_resource(
    PhenoQueryAPI,
    '/ukbrest/api/v1.0/query',
)

@app.before_first_request
def setup_logging():
    if not app.debug:
        # In production mode, add log handler to sys.stderr.
        app.logger.addHandler(logging.StreamHandler())
        app.logger.setLevel(logging.INFO)


@app.errorhandler(UkbRestException)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    from ukbrest.common.genoquery import GenoQuery
    from ukbrest.common.pheno2sql import Pheno2SQL
    from ukbrest.common.yaml_query import PhenoQuery, EHRQuery
    from ukbrest.common.utils.auth import PasswordHasher
    from ukbrest import config
    from ukbrest.common.utils.misc import update_parameters_from_args, parameter_empty

    logger = config.logger
    parser = config.get_argparse_arguments()

    args = parser.parse_args()

    # GenoQuery
    genoq_parameters = config.get_genoquery_parameters()
    genoq_parameters = update_parameters_from_args(genoq_parameters, args)

    if parameter_empty(genoq_parameters, 'genotype_path'):
        logger.warning('--genotype-path missing')

    genoq = GenoQuery(**genoq_parameters)
    app.config.update({'genoquery': genoq})

    # PhenoQuery
    pheno_query_parameters = config.get_pheno_query_parameters()
    pheno_query_parameters = update_parameters_from_args(pheno_query_parameters,
                                                         args)

    if parameter_empty(pheno_query_parameters, 'db_uri'):
        parser.error('--db-uri missing')

    pheno_query = PhenoQuery(**pheno_query_parameters)

    app.config.update({'pheno_query': pheno_query})

    # EHRQuery
    ehr_query_parameters = config.get_ehr_query_parameters()
    ehr_query_parameters = update_parameters_from_args(ehr_query_parameters,
                                                       args)
    if parameter_empty(ehr_query_parameters, 'db_uri'):
        parser.error("--db-uri missing")

    ehr_query = EHRQuery(**ehr_query_parameters)
    app.config.update({'ehr_query': ehr_query})

    ph = PasswordHasher(args.users_file, method='pbkdf2:sha256')
    ph.process_users_file()
    auth = ph.setup_http_basic_auth()
    app.config.update({'auth': auth})

    app.run(host=str(args.host), port=args.port, debug=args.debug, ssl_context='adhoc' if args.ssl_mode else None)
