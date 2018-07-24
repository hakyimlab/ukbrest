import os
import json

import werkzeug
from flask import current_app as app, Response
from flask_restful import Api

from ukbrest.common.utils.datagen import get_temp_file_name
from ukbrest.resources.ukbrestapi import UkbRestAPI


class GenotypePositionsAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(GenotypePositionsAPI, self).__init__()

        self.parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)

        self.genoq = app.config['genoquery']

    def get(self, chr, start, stop=None):
        return self.genoq.get_incl_range(chr, start, stop)

    def post(self, chr):
        args = self.parser.parse_args()

        file = get_temp_file_name('.txt')
        args.file.save(file)

        return self.genoq.get_incl_range_from_file(chr, file)


class GenotypeRsidsAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(GenotypeRsidsAPI, self).__init__()

        self.parser.add_argument('file', type=werkzeug.datastructures.FileStorage, location='files', required=True)

        self.genoq = app.config['genoquery']

    def post(self, chr):
        args = self.parser.parse_args()

        file = get_temp_file_name('.txt')
        args.file.save(file)

        return self.genoq.get_incl_rsids(chr, file)


def generate(file_path, file_mode='rb', delete=False):
    with open(file_path, mode=file_mode) as file_handle:
        chunk = True
        while chunk:
            # FIXME: buffer size hardcoded
            chunk = file_handle.read(8192)
            if chunk:
                yield chunk

    if delete:
        os.remove(file_path)


def output_bgen(bgen_filepath, code, headers=None):
    resp = Response(generate(bgen_filepath, delete=True), code)
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