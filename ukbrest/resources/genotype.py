from flask import Response, current_app as app
from flask_restful import Resource, reqparse
import werkzeug

from utils.datagen import get_temp_file_name


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
