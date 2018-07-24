from flask import jsonify
from flask_restful import Resource, reqparse, current_app as app, Api

from ukbrest.resources.exceptions import UkbRestException


class UkbRestAPI(Resource):
    HTTP_METHODS = ('get', 'post')

    def __init__(self):
        self.parser = reqparse.RequestParser()

        # add error handling
        for met in UkbRestAPI.HTTP_METHODS:
            if hasattr(self, met):
                setattr(self, met, self.handle_errors(getattr(self, met)))

        # add http authentication
        if 'auth' in app.config and app.config['auth'] is not None:
            auth = app.config['auth']

            for met in UkbRestAPI.HTTP_METHODS:
                if met in dir(self):
                    setattr(self, met, auth.login_required(getattr(self, met)))

    def handle_errors(self, func):
        def func_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except UkbRestException as e:
                return self._make_ukbrest_error(e)

        return func_wrapper

    def _make_ukbrest_error(self, ukbrest_exception):
        response = jsonify({
            'status_code': ukbrest_exception.status_code,
            'message': ukbrest_exception.message,
            'output': ukbrest_exception.output,
        })

        response.status_code = ukbrest_exception.status_code

        return response
