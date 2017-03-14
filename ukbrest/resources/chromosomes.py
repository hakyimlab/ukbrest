from flask import Response
from flask_restful import Resource, reqparse
from os.path import join





class ChromosomeAPI(Resource):
    def __init__(self):
        # self.parser = reqparse.RequestParser()
        # self.parser.add_argument('info', type=int, help='Rate to charge for this resource')

        super(ChromosomeAPI, self).__init__()

    def get(self, code):
        """
        Returns the entire chromosome file.
        :return:
        """
        # args = self.parser.parse_args()

        def generate():
            with open(join('/tmp/data', str(code) + '.bgen'), "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if chunk:
                        yield chunk
                    else:
                        break

        return Response(generate(), mimetype='application/octet-stream')


class ChromosomeInfoAPI(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('info', action='store_true')

        super(ChromosomeInfoAPI, self).__init__()

    def get(self, code):
        """
        Returns info about a chromosome file.
        :return:
        """

        args = self.parser.parse_args()

        return {'name': 'Milton', 'lastname': 'Pividori'}
