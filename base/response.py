import datetime
import json, os
from urllib.parse import quote

import decimal
from flask import make_response
from base.exceptions import UnAuthorizedException

CONTENT_TYPES = {
    "xml": "application/xml",
    "json": "application/json",
    "text": "text/plain",
    "png": "image/png",
    "jpg": "image/jpg",
    "gif": "image/gif",
}

CONTENT_ATT_TYPES = {
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.ms-excel",
    ".zip": "application/zip",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".ppt": "application/vnd.ms-powerpoint",
    ".doc": "application/msword",
    ".docx": "application/msword",
    ".jpg": "image/jpg",
    ".gif": "image/gif",

}


def make_custom_response(*args, htype="text"):
    response = make_response(*args)
    response.headers["Content-Type"] = CONTENT_TYPES[htype]
    return response


def make_attachment_response(args, filename):
    response = make_response(args)
    file_ext = os.path.splitext(filename)[1].lower()
    response.headers["Content-type"] = "{}; charset=UTF-8".format(CONTENT_ATT_TYPES[file_ext])
    response.headers["Content-Disposition"] = "attachment; filename*=utf-8''" + quote(
        ("%s" % (filename,)).encode("utf-8"))
    return response


def make_text(*args):
    return make_custom_response(*args)


def make_xml(*args):
    return make_custom_response(*args, htype="xml")


def make_png(*args):
    return make_custom_response(*args, htype="png")

def make_pdf(args, filename):
    return make_attachment_response(args, filename)

def make_excel(args, filename):
    return make_attachment_response(args, filename)


def make_zip(args, filename):
    return make_attachment_response(args, filename)

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return json.JSONEncoder.default(self, obj)

origin_dumps = json.dumps


def dumps(*args, **kwargs):
    if "cls" in kwargs:
        try:
            return origin_dumps(*args, **kwargs)
        except:
            kwargs.pop("cls")
    kwargs["cls"] = ComplexEncoder
    return origin_dumps(*args, **kwargs)

json.dumps = dumps

def make_json(response):
    if not isinstance(response, (str, bytes)):
        response = json.dumps(response)
    return make_custom_response(response, htype="json")


def response_json(data=None, code=0, msg=''):
    return make_json({'code': code, 'data': data, 'msg': msg})
