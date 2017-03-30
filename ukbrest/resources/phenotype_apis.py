import tempfile
import json
from flask import Response
from flask_restful import Api


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
    data.to_csv(f, sep='\t')
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
