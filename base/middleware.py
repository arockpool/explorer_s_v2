import logging
from flask import g
from werkzeug.exceptions import HTTPException

from base.exceptions import UnAuthorizedException, ValidateError
from base.openapi.exception import OpenApiException
from base.response import response_json


def intercept_assert_request(exception):
    return response_json(code=-1, msg=','.join(exception.args))


def intercept_unauthorized_request(exception):
    return response_json(code=-5, msg=','.join(exception.args))


def intercept_openapi_exception(exception):
    return response_json(code=exception.code, msg=exception.msg)


def intercept_validate_exception(exception):
    return response_json(code=-10, msg=exception.msg)


def intercept_internal_err_request(exception):
    if isinstance(exception, HTTPException) and exception.code < 500:
        raise exception
    logging.exception(exception)
    return response_json(code=-1, msg="系统繁忙，请稍后再试！")


# def before_request():
#     g.range_dp_ids = set()
#     g.range_enterprise_ids = set()
#     g.is_super = False
#     g.range_wa_ids = set()
#     g.range_pa_ids = set()


def init(app):
    app.register_error_handler(OpenApiException, intercept_openapi_exception)
    app.register_error_handler(AssertionError, intercept_assert_request)
    app.register_error_handler(UnAuthorizedException, intercept_unauthorized_request)
    app.register_error_handler(ValidateError, intercept_validate_exception)
    app.register_error_handler(Exception, intercept_internal_err_request)
    # app.before_request(before_request)
