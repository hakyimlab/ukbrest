import json

import werkzeug
from flask import current_app as app, Response
from flask_restful import Resource, reqparse, Api

from ukbrest.common.utils.datagen import get_temp_file_name


class GenotypePositionsAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)

        self.genoq = app.config['genoquery']

        super(GenotypePositionsAPI, self).__init__()

    def get(self, chr, start, stop=None):
        return self.genoq.get_incl_range(chr, start, stop)

    def post(self, chr):
        args = self.parser.parse_args()

        file = get_temp_file_name('.txt')
        args.file.save(file)

        return self.genoq.get_incl_range_from_file(chr, file)


class GenotypeRsidsAPI(Resource):
    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)

        self.genoq = app.config['genoquery']

        super(GenotypeRsidsAPI, self).__init__()

    def post(self, chr):
        args = self.parser.parse_args()

        file = get_temp_file_name('.txt')
        args.file.save(file)

        return self.genoq.get_incl_rsids(chr, [file])


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