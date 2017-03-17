from os.path import isdir
from flask import Flask
from flask_restful import Api

from common.ukbquery import UKBQuery
from ukbrest.resources.chromosomes import ChromosomeAPI
from ukbrest.resources.variants import VariantAPI

app = Flask(__name__)
# api_bp = Blueprint('api', __name__)
api = Api(app)

api.add_resource(
    ChromosomeAPI,
    '/ukbrest/api/v1.0/chromosomes/<int:chr>/variants/<int:start>/<int:stop>'
)

if __name__ == '__main__':
    from ipaddress import ip_address
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('repository_path', type=str, help='UK Biobank repository path')
    parser.add_argument('--host', dest='host', type=ip_address, required=False, help='Host', default=ip_address('127.0.0.1'))
    parser.add_argument('--port', dest='port', type=int, required=False, help='Port', default=5000)

    args = parser.parse_args()

    if not isdir(args.repository_path):
        raise Exception('Repository path does not exist: {}'.format(args.repository_path))

    app.config.update({'ukbquery': UKBQuery(args.repository_path)})

    app.run(host=str(args.host), port=args.port, debug=True)
