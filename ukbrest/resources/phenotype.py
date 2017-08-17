import json
import tempfile

from flask import Response
from flask_restful import Resource, reqparse, current_app as app, Api


class PhenotypeAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('columns', type=str, action='append', required=True, help='Columns to include')
        self.parser.add_argument('filters', type=str, action='append', required=False, help='Filters to include (AND)')
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                 help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeAPI, self).__init__()

    def get(self):
        args = self.parser.parse_args()

        return self.pheno2sql.query(args.columns, args.filters)


class PhenotypeFieldsAPI(Resource):
    def __init__(self, **kwargs):
        # self.parser = reqparse.RequestParser()
        # self.parser.add_argument('info', type=int, help='Rate to charge for this resource')

        self.pheno2sql = app.config['pheno2sql']

        super(PhenotypeFieldsAPI, self).__init__()

    def get(self):
        # FIXME: should not return Response nor json, just raw data
        # FIXME: accessing protected field _db_types
        return [k for k, v in self.pheno2sql._db_dtypes.items()]


def _data_generator(all_data, data_conversion_func):
    from io import StringIO

    for row_idx, row in enumerate(all_data):
        f = StringIO()
        data_conversion_func(row, row_idx, f)

        yield f.getvalue()


def _to_phenotype(data, data_chunk_idx, buffer):
    data.index.name = 'FID'
    data = data.assign(IID=data.index.values.copy())

    columns = data.columns.tolist()
    columns_reordered = ['IID'] + [c for c in columns if c != 'IID']
    data = data.loc[:, columns_reordered]

    data.to_csv(buffer, sep='\t', na_rep='NA', header=data_chunk_idx == 0)


def output_phenotype(data, code, headers=None):
    resp = Response(_data_generator(data, _to_phenotype), code)
    resp.headers.extend(headers or {})
    return resp


def _to_csv(data, data_chunk_idx, buffer):
    data.to_csv(buffer, header=data_chunk_idx == 0)


def output_csv(data, code, headers=None):
    resp = Response(_data_generator(data, _to_csv), code)
    resp.headers.extend(headers or {})
    return resp


def output_json(data, code, headers=None):
    resp = Response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


PHENOTYPE_FORMATS = {
    'text/phenotype': output_phenotype,
    'text/csv': output_csv,
}


class PhenotypeApiObject(Api):
    def __init__(self, app):
        super(PhenotypeApiObject, self).__init__(app, default_mediatype='text/phenotype')

        reps = PHENOTYPE_FORMATS.copy()
        reps.update({'application/json': output_json})
        self.representations = reps