import json

from ruamel.yaml import YAML
from werkzeug.exceptions import BadRequest
from werkzeug.datastructures import FileStorage
from flask import Response
from flask_restful import Resource, reqparse, current_app as app, Api

from ukbrest.common.utils.constants import BGEN_SAMPLES_TABLE


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
        if args.Accept == 'text/bgenie':
            order_by_table = BGEN_SAMPLES_TABLE

        data_results = self.pheno2sql.query_yaml(yaml.load(args.file), args.section, order_by_table=order_by_table)

        return {
            'data': data_results,
            'missing_code': args.missing_code,
        }


def _data_generator(all_data, data_conversion_func):
    from io import StringIO

    for row_idx, row in enumerate(all_data):
        f = StringIO()
        data_conversion_func(row, row_idx, f)

        yield f.getvalue()


def output_phenotype(data, code, headers=None):
    def _to_phenotype(data, data_chunk_idx, buffer):
        data.index.name = 'FID'
        data = data.assign(IID=data.index.values.copy())

        columns = data.columns.tolist()
        columns_reordered = ['IID'] + [c for c in columns if c != 'IID']
        data = data.loc[:, columns_reordered]

        data.to_csv(buffer, sep='\t', na_rep='NA', header=data_chunk_idx == 0)

    resp = Response(_data_generator(data['data'], _to_phenotype), code)
    resp.headers.extend(headers or {})
    return resp


def output_bgenie(data, code, headers=None):
    missing_code = data['missing_code'] if ('missing_code' in data) and (data['missing_code'] is not None) else 'NA'

    def _to_bgenie(data_chunk, data_chunk_idx, buffer):
        data_chunk.to_csv(buffer, sep=' ', na_rep=missing_code, header=data_chunk_idx == 0, index=False)

    resp = Response(_data_generator(data['data'], _to_bgenie), code)
    resp.headers.extend(headers or {})
    return resp


def output_csv(data, code, headers=None):
    def _to_csv(data, data_chunk_idx, buffer):
        data.to_csv(buffer, header=data_chunk_idx == 0)

    resp = Response(_data_generator(data['data'], _to_csv), code)
    resp.headers.extend(headers or {})
    return resp


def output_json(data, code, headers=None):
    if isinstance(data, dict) and 'data' in data:
        data = data['data']

    resp = Response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


PHENOTYPE_FORMATS = {
    'text/phenotype': output_phenotype,
    'text/csv': output_csv,
    'text/bgenie': output_bgenie,
}


class PhenotypeApiObject(Api):
    def __init__(self, app, default_mediatype='text/phenotype'):
        super(PhenotypeApiObject, self).__init__(app, default_mediatype=default_mediatype)

        reps = PHENOTYPE_FORMATS.copy()
        reps.update({'application/json': output_json})
        self.representations = reps
