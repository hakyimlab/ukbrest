from ruamel.yaml import YAML
from werkzeug.exceptions import BadRequest
from werkzeug.datastructures import FileStorage
from flask_restful import Resource, reqparse, current_app as app, Api

from ukbrest.resources.formats import CSVSerializer, BgenieSerializer, Plink2Serializer, JsonSerializer


PHENOTYPE_FORMATS = {
    'text/plink2': Plink2Serializer(),
    'text/csv': CSVSerializer(),
    'text/bgenie': BgenieSerializer(),
}


class PhenotypeAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('columns', type=str, action='append', required=False, help='Columns to include')
        self.parser.add_argument('ecolumns', type=str, action='append', required=False, help='Columns to include (with regular expressions)')
        self.parser.add_argument('filters', type=str, action='append', required=False, help='Filters to include (AND)')
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                 help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeAPI, self).__init__()

    def get(self):
        args = self.parser.parse_args()

        if args.columns is None and args.ecolumns is None:
            raise BadRequest('You have to specify either columns or ecolumns')

        data_results = self.pheno2sql.query(args.columns, args.ecolumns, args.filters)

        return {
            'data': data_results,
        }


class PhenotypeFieldsAPI(Resource):
    def __init__(self, **kwargs):
        # self.parser = reqparse.RequestParser()
        # self.parser.add_argument('info', type=int, help='Rate to charge for this resource')

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeFieldsAPI, self).__init__()

    def get(self):
        self.pheno2sql.get_field_dtype()

        data_results = [k for k, v in self.pheno2sql._fields_dtypes.items()]

        return {
            'data': data_results,
        }


class QueryAPI(Resource):
    def __init__(self, **kwargs):
        self.post_parser = reqparse.RequestParser()
        self.post_parser.add_argument('file', type=FileStorage, location='files', required=True)
        self.post_parser.add_argument('section', type=str, required=True)
        self.post_parser.add_argument('missing_code', type=str, required=False)
        self.post_parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                      help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

        super(QueryAPI, self).__init__()

    def post(self):
        args = self.post_parser.parse_args()

        yaml = YAML(typ='safe')

        order_by_table = None
        if args.Accept in PHENOTYPE_FORMATS:
            serializer = PHENOTYPE_FORMATS[args.Accept]
            order_by_table = serializer.get_order_by_table()

        data_results = self.pheno2sql.query_yaml(
            yaml.load(args.file),
            args.section,
            order_by_table=order_by_table
        )

        final_results = {
            'data': data_results,
        }

        if args.missing_code is not None:
            final_results['missing_code'] = args.missing_code

        return final_results


class PhenotypeApiObject(Api):
    def __init__(self, app, default_mediatype='text/plink2'):
        super(PhenotypeApiObject, self).__init__(app, default_mediatype=default_mediatype)

        reps = PHENOTYPE_FORMATS.copy()
        reps.update({'application/json': JsonSerializer()})
        self.representations = reps
