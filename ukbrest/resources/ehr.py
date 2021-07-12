from ruamel.yaml import YAML
from werkzeug.datastructures import FileStorage
from flask_restful import current_app as app, Api

from ukbrest.resources.exceptions import UkbRestValidationError
from ukbrest.resources.ukbrestapi import UkbRestAPI
from ukbrest.resources.formats import (CSVSerializer, BgenieSerializer,
                                       Plink2Serializer, JsonSerializer)

EHR_FORMATS = {
    'text/csv': CSVSerializer()
}

class EHRApiObject(Api):
    def __init__(self, app, default_mediatype="text/csv"):
        super(EHRApiObject, self).__init__(app,
                                           default_mediatype=default_mediatype)
        self.representations = EHR_FORMATS.copy()


class EHRAPI(UkbRestAPI):
    def __init__(self, **kwargs):
        super(EHRAPI, self).__init__()

