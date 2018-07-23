from types import MethodType

from flask_restful import Resource, reqparse, current_app as app, Api


class UkbRestAPI(Resource):
    AUTHENTICATED_METHOD = ('get', 'post')

    def __init__(self):
        self.parser = reqparse.RequestParser()

        if 'auth' in app.config and app.config['auth'] is not None:
            auth = app.config['auth']

            for met in UkbRestAPI.AUTHENTICATED_METHOD:
                if met in dir(self):
                    setattr(self, met, auth.login_required(getattr(self, met)))
