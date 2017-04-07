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
        return [k for k, v in self.pheno2sql.columns_and_dtypes.items()]


def generate(file_handle):
    while True:
        # FIXME: buffer size hardcoded
        chunk = file_handle.read(8192)
        if chunk:
            yield chunk
        else:
            break


def output_phenotype(data, code, headers=None):
    data.index.names = ['FID']
    data = data.assign(IID=data.index.values.copy())

    columns = data.columns.tolist()
    columns_reordered = ['IID'] + [c for c in columns if c !='IID']
    data = data.loc[:, columns_reordered]

    f = tempfile.TemporaryFile(mode='r+')
    data.to_csv(f, sep='\t', na_rep='NA')
    f.seek(0)

    resp = Response(generate(f), code)
    resp.headers.extend(headers or {})
    return resp


def output_csv(data, code, headers=None):
    f = tempfile.TemporaryFile(mode='r+')
    data.to_csv(f)
    f.seek(0)

    resp = Response(generate(f), code)
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