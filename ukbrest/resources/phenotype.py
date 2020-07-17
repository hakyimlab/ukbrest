from ruamel.yaml import YAML
from werkzeug.datastructures import FileStorage
from flask_restful import current_app as app, Api

from ukbrest.resources.exceptions import UkbRestValidationError
from ukbrest.resources.ukbrestapi import UkbRestAPI
from ukbrest.resources.formats import CSVSerializer, BgenieSerializer, Plink2Serializer, JsonSerializer


PHENOTYPE_FORMATS = {
    'text/plink2': Plink2Serializer(),
    'text/csv': CSVSerializer(),
    'text/bgenie': BgenieSerializer(),
}


class PhenotypeAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(PhenotypeAPI, self).__init__()

        self.parser.add_argument('columns', type=str, action='append', required=False, help='Columns to include')
        self.parser.add_argument('ecolumns', type=str, action='append', required=False, help='Columns to include (with regular expressions)')
        self.parser.add_argument('filters', type=str, action='append', required=False, help='Filters to include (AND)')
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                 help='Only {} are supported'.format(', '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

    def get(self):
        args = self.parser.parse_args()

        if args.columns is None and args.ecolumns is None:
            raise UkbRestValidationError('You have to specify either columns or ecolumns')

        data_results = self.pheno2sql.query(args.columns, args.ecolumns, args.filters)

        return {
            'data': data_results,
        }


class PhenotypeFieldsAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(PhenotypeFieldsAPI, self).__init__()

        self.pheno2sql = app.config['pheno2sql']

    def get(self):
        self.pheno2sql.get_field_dtype()

        data_results = [k for k, v in self.pheno2sql._fields_dtypes.items()]

        return {
            'data': data_results,
        }


class QueryAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(QueryAPI, self).__init__()

        self.parser.add_argument('file', type=FileStorage, location='files', required=True)
        self.parser.add_argument('section', type=str, required=True)
        self.parser.add_argument('missing_code', type=str, required=False)
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                      help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

    def post(self):
        args = self.parser.parse_args()

        yaml = YAML(typ='safe')

        order_by_table = None
        if args.Accept in PHENOTYPE_FORMATS:
            serializer = PHENOTYPE_FORMATS[args.Accept]
            order_by_table = serializer.get_order_by_table()
        print(args.Accept)
        print(args.file)
        print(args.section)
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
