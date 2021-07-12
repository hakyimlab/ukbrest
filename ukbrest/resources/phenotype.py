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
        self.pheno_query = app.config['pheno_query']

    def get(self):
        args = self.parser.parse_args()

        if args.columns is None and args.ecolumns is None:
            raise UkbRestValidationError('You have to specify either columns or ecolumns')

        data_results = self.pheno_query.query(args.columns, args.ecolumns, args.filters)

        return {
            'data': data_results,
        }


class PhenotypeFieldsAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(PhenotypeFieldsAPI, self).__init__()

        self.pheno_query = app.config['pheno_query']

    def get(self):
        self.pheno_query.get_field_dtype()

        data_results = [k for k, v in self.pheno_query._fields_dtypes.items()]

        return {
            'data': data_results,
        }


class PhenotypeApiObject(Api):
    def __init__(self, app, default_mediatype='text/plink2'):
        super(PhenotypeApiObject, self).__init__(app, default_mediatype=default_mediatype)

        reps = PHENOTYPE_FORMATS.copy()
        reps.update({'application/json': JsonSerializer()})
        self.representations = reps
