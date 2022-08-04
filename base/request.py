from functools import wraps

from flask import request, session, g

from .flask_ext import cache
from base.response import response_json

from base.utils.json import values_to_json


def authenticated(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        pass
        return method(*args, **kwargs)

    return wrapper


