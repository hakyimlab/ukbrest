from flask import Response, current_app as app
from flask_restful import Resource


class ChromosomeAPI(Resource):
    def __init__(self, **kwargs):
        # self.parser = reqparse.RequestParser()
        # self.parser.add_argument('info', type=int, help='Rate to charge for this resource')

        self.ukbquery = app.config['ukbquery']

        super(ChromosomeAPI, self).__init__()

    def get(self, chr, start, stop):
        """
        Returns the chromosome's variants between positions start and stop.
        :return:
        """
        # args = self.parser.parse_args()

        def generate(file):
            with open(file, "rb") as f:
                while True:
                    chunk = f.read(8192)
                    if chunk:
                        yield chunk
                    else:
                        break

        bgen_file = self.ukbquery.get_incl_range(chr, start, stop)

        return Response(generate(bgen_file), mimetype='application/octet-stream')
