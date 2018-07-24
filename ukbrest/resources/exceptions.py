
class UkbRestException(Exception):
    def __init__(self, message, subtype, output=None):
        super(UkbRestException, self).__init__(message)

        self.message = message
        self.status_code = 400
        self.subtype = subtype
        self.output = output


class UkbRestValidationError(UkbRestException):
    def __init__(self, message):
        super(UkbRestValidationError, self).__init__(message, 'VALIDATION_ERROR')


class UkbRestExecutionError(UkbRestException):
    def __init__(self, message, output):
        super(UkbRestExecutionError, self).__init__(message, 'EXECUTION_ERROR', output)
