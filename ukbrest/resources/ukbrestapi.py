from flask_restful import Resource, reqparse, current_app as app

from ukbrest.resources.error_handling import handle_http_errors


class UkbRestAPI(Resource):
    HTTP_METHODS = ('get', 'post')

    def __init__(self):
        self.parser = reqparse.RequestParser()

        # add error handling
        for met in UkbRestAPI.HTTP_METHODS:
            if hasattr(self, met):
                setattr(self, met, handle_http_errors(getattr(self, met)))

        # add http authentication
        if 'auth' in app.config and app.config['auth'] is not None:
            auth = app.config['auth']

            for met in UkbRestAPI.HTTP_METHODS:
                if met in dir(self):
                    setattr(self, met, auth.login_required(getattr(self, met)))
