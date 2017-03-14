from flask import Flask, Blueprint
from flask_restful import Api
from ukbrest.resources.chromosomes import ChromosomeAPI, ChromosomeInfoAPI
from ukbrest.resources.variants import VariantAPI

app = Flask(__name__)
api_bp = Blueprint('api', __name__)
api = Api(app)

api.add_resource(ChromosomeAPI, '/ukbrest/api/v1.0/chromosomes/<int:code>')
api.add_resource(ChromosomeInfoAPI, '/ukbrest/api/v1.0/chromosomes/<int:code>/info')
api.add_resource(VariantAPI, '/ukbrest/api/v1.0/variants')

# app.register_blueprint(api_bp)

# @app.route('/ukbrest/api/v1.0/files/<string:filename>', methods=['GET'])
# def generate_large_csv(filename):
#     def generate():
#         with open(join('/Users/miltondp/projects/expression/data', filename), "rb") as f:
#             while True:
#                 chunk = f.read(8192)
#                 if chunk:
#                     yield chunk
#                 else:
#                     break
#
#     return Response(generate(), mimetype='application/octet-stream')
#
#
# @app.route('/ukbrest/api/v1.0/chromosomes/<int:chromosome>/variants/<string:rsid>', methods=['GET'])
# def generate_large_csv(chromosome, rsid):
#     pass
#
#     return Response(generate(), mimetype='application/octet-stream')

if __name__ == '__main__':
    app.run(debug=True)
