import traceback
from joblib.my_exceptions import JoblibException
from sqlalchemy.exc import ProgrammingError

from flask import jsonify
from werkzeug.exceptions import HTTPException

from ukbrest.resources.exceptions import UkbRestException
from ukbrest.config import logger


def handle_http_errors(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except UkbRestException as e:
            return _make_ukbrest_error(e)
        except ProgrammingError as e:
            return _make_ukbrest_error(e)
        except Exception as e:
            return _make_ukbrest_error(e)
    return func_wrapper


def handle_errors(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except UkbRestException as e:
            pass
        except Exception as e:
            tb = traceback.format_exc()
            logger.debug(tb)

            msg = f'\n{str(e)}'
            # if isinstance(e, JoblibException):
            #     msg = ''

            logger.error(f'Loading finished with an unknown error. Activate debug to see full stack trace.{msg}')

    return func_wrapper


def _make_ukbrest_error(ukbrest_exception):
    response_dict = {}

    status_code = 500

    if isinstance(ukbrest_exception, UkbRestException):
        status_code = ukbrest_exception.status_code

        response_dict['status_code'] = status_code
        response_dict['error_type'] = ukbrest_exception.subtype
        response_dict['message'] = ukbrest_exception.message

        if hasattr(ukbrest_exception, 'output'):
            response_dict['output'] = ukbrest_exception.output

    elif isinstance(ukbrest_exception, HTTPException):
        status_code = ukbrest_exception.code

        response_dict['status_code'] = status_code
        response_dict['error_type'] = 'UNKNOWN'

        if hasattr(ukbrest_exception, 'data') and isinstance(ukbrest_exception.data, dict):
            response_dict.update(ukbrest_exception.data)

    else:
        response_dict['status_code'] = status_code
        response_dict['error_type'] = 'UNKNOWN'
        response_dict['message'] = str(ukbrest_exception)

    response = jsonify(response_dict)
    response.status_code = status_code

    return response
