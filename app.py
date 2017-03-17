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
    '/ukbrest/api/v1.0/chromosomes/<int:chr>/positions/<int:start>/<int:stop>'
)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('repository_path', type=str, help='UK Biobank repository path')

    args = parser.parse_args()

    app.config.update({'ukbquery': UKBQuery(args.repository_path)})

    app.run(debug=True)
