from os.path import isdir
from flask import Flask
from flask_restful import Api

from common.apis import PhenotypeApiObject
from common.pheno2sql import Pheno2SQL
from common.ukbquery import UKBQuery
from ukbrest.resources.chromosomes import ChromosomeAPI
from ukbrest.resources.phenotype import PhenotypeFieldsAPI, PhenotypeAPI

app = Flask(__name__)


# Genotype API
genotype_api = Api(app, default_mediatype='application/octet-stream')

genotype_api.add_resource(
    ChromosomeAPI,
    '/ukbrest/api/v1.0/chromosomes/<int:chr>/variants/<int:start>/<int:stop>'
)

# Phenotype API
phenotype_api = PhenotypeApiObject(app)

phenotype_api.add_resource(
    PhenotypeAPI,
    '/ukbrest/api/v1.0/phenotype'
)

# Phenotype Info API
phenotype_info_api = Api(app, default_mediatype='application/json')

phenotype_api.add_resource(
    PhenotypeFieldsAPI,
    '/ukbrest/api/v1.0/phenotype/fields'
)

if __name__ == '__main__':
    from ipaddress import ip_address
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('genotype_path', type=str, help='UK Biobank genotype (imputed) path')
    parser.add_argument('phenotype_csv', type=str, help='UK Biobank phenotype data in CSV format')
    parser.add_argument('--host', dest='host', type=ip_address, required=False, help='Host', default=ip_address('127.0.0.1'))
    parser.add_argument('--port', dest='port', type=int, required=False, help='Port', default=5000)
    parser.add_argument('--debug', dest='debug', action='store_true')

    args = parser.parse_args()

    if not isdir(args.genotype_path):
        raise Exception('Repository path does not exist: {}'.format(args.genotype_path))

    app.config.update({'ukbquery': UKBQuery(args.genotype_path, debug=args.debug)})

    csv_file = 'tests/data/pheno2sql/example02.csv'
    db_engine = 'postgresql://test:test@localhost:5432/ukb'
    p2sql = Pheno2SQL(csv_file, db_engine, n_columns_per_table=2)
    p2sql.load_data()

    app.config.update({'pheno2sql': p2sql})

    app.run(host=str(args.host), port=args.port, debug=args.debug)
