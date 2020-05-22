from flask_restful import current_app as app, Api
from werkzeug.datastructures import FileStorage
from ruamel.yaml import YAML
from ruamel.yaml.scanner import ScannerError
import pandas as pd
import re
from sqlalchemy.exc import ProgrammingError

from ukbrest.common.utils.db import DBAccess
from ukbrest.resources.exceptions import UkbRestSQLExecutionError, UkbRestProgramExecutionError
from ukbrest.config import logger
from ukbrest.resources.formats import CSVSerializer, BgenieSerializer, Plink2Serializer, JsonSerializer
from ukbrest.resources.ukbrestapi import UkbRestAPI

PHENOTYPE_FORMATS = {
    'text/plink2': Plink2Serializer(),
    'text/csv': CSVSerializer(),
    'text/bgenie': BgenieSerializer(),
}

class QueryApiObject(Api):
    def __init__(self, app, default_mediatype='text/plink2'):
        super(QueryApiObject, self).__init__(app, default_mediatype=default_mediatype)

        reps = PHENOTYPE_FORMATS.copy()
        reps.update({'application/json': JsonSerializer()})
        self.representations = reps


class PhenoQueryAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(PhenoQueryAPI, self).__init__()

        self.parser.add_argument('file', type=FileStorage, location='files', required=True)
        self.parser.add_argument('section', type=str, required=True)
        self.parser.add_argument('missing_code', type=str, required=False)
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                      help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.pheno_query = app.config['pheno_query']

    def post(self):
        args = self.parser.parse_args()

        yaml = YAML(typ='safe')

        order_by_table = None
        if args.Accept in PHENOTYPE_FORMATS:
            serializer = PHENOTYPE_FORMATS[args.Accept]
            order_by_table = serializer.get_order_by_table()
        print(args.Accept)
        print(args.file)
        print(args.section)
        data_results = self.pheno_query.query_yaml(
            yaml.load(args.file),
            args.section,
            order_by_table=order_by_table
        )

        final_results = {
            'data': data_results,
        }

        if args.missing_code is not None:
            final_results['missing_code'] = args.missing_code

        return final_results

class EHRQueryAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(EHRQueryAPI, self).__init__()

        self.parser.add_argument('file', type=FileStorage, location='files', required=True)
        self.parser.add_argument('section', type=str, required=True)
        self.parser.add_argument('missing_code', type=str, required=False)
        self.parser.add_argument('Accept', location='headers', choices=PHENOTYPE_FORMATS.keys(),
                                      help='Only {} are supported'.format(' and '.join(PHENOTYPE_FORMATS.keys())))

        self.ehr_query = app.config['ehr_query']

    def post(self):
        args = self.parser.parse_args()

        yaml = YAML(typ='safe')
        serializer = PHENOTYPE_FORMATS[args.Accept]
        data_results = self.ehr_query.query_yaml(
            yaml.load(args.file),
            args.section
        )
        final_results = {'data': data_results}
        if args.missing_code is not None:
            final_results['missing_code'] = args.missing_code

        return final_results
