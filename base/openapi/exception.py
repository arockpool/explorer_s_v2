class OpenApiException(Exception):

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class OpenApiValidateException(OpenApiException):

    def __init__(self, msg):
        super().__init__(10400, msg)
