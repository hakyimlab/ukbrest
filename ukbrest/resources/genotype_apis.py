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


def output_bgen(bgen_filepath, code, headers=None):
    bgen_file_handle = open(bgen_filepath, mode='rb')

    resp = Response(generate(bgen_file_handle), code)
    resp.headers.extend(headers or {})
    return resp


def output_json(data, code, headers=None):
    resp = Response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


GENOTYPE_FORMATS = {
    'application/octet-stream': output_bgen,
}


class GenotypeApiObject(Api):
    def __init__(self, app):
        super(GenotypeApiObject, self).__init__(app, default_mediatype='application/octet-stream')

        reps = GENOTYPE_FORMATS.copy()
        reps.update({'application/json': output_json})
        self.representations = reps
