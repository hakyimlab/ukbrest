import json
from flask import Response
from flask_restful import Resource, reqparse, current_app as app

from common.apis import PHENOTYPES_FORMATS


class PhenotypeAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('columns', type=str, action='append', required=True, help='Columns to include')
        self.parser.add_argument('filters', type=str, action='append', required=False, help='Filters to include (AND)')
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPES_FORMATS.keys(),
                                 help='Only {} are supported'.format(' and '.join(PHENOTYPES_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeAPI, self).__init__()

    def get(self):
        args = self.parser.parse_args()

        return self.pheno2sql.query(args.columns, args.filters)


class PhenotypeFieldsAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('info', type=int, help='Rate to charge for this resource')

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeFieldsAPI, self).__init__()

    def get(self):
        return Response(json.dumps(self.pheno2sql.fields), mimetype='text/plain')
