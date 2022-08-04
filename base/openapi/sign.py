import json
import hmac
import hashlib
import datetime
import uuid, json
from flask import request
from base.flask_ext import cache
from requests import request as do_request
import base64, time
from flask import current_app as app

ALLOWED_VERSIONS = ['V1', 'V2']


class OpenApi:
    def __init__(self, appkey, appsecret, authorization=None):
        self.appkey = appkey
        self.appsecret = appsecret
        self.authorization = authorization
        self._headers = None

    def __call__(self, appkey, appsecret):
        self.appkey = appkey
        self.appsecret = appsecret

    @property
    def headers(self):
        return self._headers

    @headers.setter
    def headers(self, headers):
        self._headers = headers

    def build_sign_sha256(self, sign_str, appsecret):
        """
        使用HMAC-SHA256签名
        :param sign_str:
        :param appsecret:
        :return:
        """
        s = hmac.new(appsecret.encode(), sign_str.encode(), digestmod=hashlib.sha256)
        signature = s.hexdigest()
        return signature

    def verify_sign_sha256(self, sign_str, verify_str, appsecret):
        """
        验证签名
        :param sign_str: 被签名的数据
        :param verify_str: 签名的结果
        :param appsecret:
        :return:
        """
        s = hmac.new(appsecret.encode(), sign_str.encode(), digestmod=hashlib.sha256)
        signature = s.hexdigest()
        if verify_str == signature:
            return True
        else:
            return False

    def verify_sign(self, data):
        """
        V1版本验证签名
        :param headers: Request header
        :param data: Request Data
        :param appsecret:
        :return:
        """
        sign_str = self.structure_data(data, request.headers.get('Timestamp'), request.headers.get('SignatureNonce'))
        result = self.verify_sign_sha256(sign_str, request.headers.get('Signature'), self.appsecret)
        return result

    def sign(self, headers, data):
        """
        V1生产验证签名
        :param headers:
        :param data: Request Data
        :param appsecret:
        :return:
        """
        sign_str = self.structure_data(data, headers.get('Timestamp'), headers.get('SignatureNonce'))
        return self.build_sign_sha256(sign_str, self.appsecret)

    def build_headers(self, method, url, data=None):
        """
        构建回调 headers
        :param appsecret:
        :param body:
        :return:
        """
        headers = {
            'AppId': self.appkey,
            'Authorization': self.authorization,
            'Timestamp': datetime.datetime.now().astimezone().strftime('%Y-%m-%dT%H:%M:%S%z'),
            'SignatureVersion': 'V1',
            'SignatureMethod': 'HMAC-SHA256',
            'SignatureNonce': uuid.uuid4().hex,
            'Signature': '',
        }

        if method.lower() in ['get', 'delete']:
            payload = ''
        if method.lower() in ['post', 'put']:
            payload = json.dumps(data or {})

        sing = self.sign(headers, payload)
        headers['Signature'] = sing
        self.headers = headers
        return headers

    def send(self, method, host, uri, data=None):
        headers = self.build_headers(method, uri, data)
        response = do_request(method=method, url=host + uri, json=data, headers=headers)
        return response.status_code, response.json()

    @classmethod
    def check_header(cls):
        headers = request.headers
        arg_list = ['AppId', 'Timestamp', 'SignatureVersion', 'SignatureMethod', 'SignatureNonce', 'Signature']
        for arg in arg_list:
            if not headers.get(arg):
                return 10400, 'headers数据校验失败，缺少参数:{0}'.format(arg)
        if headers.get('SignatureVersion') not in ALLOWED_VERSIONS:
            return 10001, '接口不支持的版本'
        # YYYY-MM-DDThh:mm:ssZ，T表示间隔，Z代表0时区
        timestamp = datetime.datetime.strptime(headers.get('Timestamp'), '%Y-%m-%dT%H:%M:%S%z')
        utcnow = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(timestamp.tzinfo)
        if utcnow > timestamp + datetime.timedelta(seconds=app.config.get('OPEN_API_TIMEOUT', 60)):
            return 10403, '请求被拒绝，请求超时'
        nonce = headers.get('SignatureNonce')
        if cache.cache.add(nonce, True, 3600 * 24 * 5):
            pass
        else:
            return 10403, '请求被拒绝，SignatureNonce 已经存在'
        return 0, ''


class V1(OpenApi):
    def structure_data(self, json_str, timestamp, sign_nonce):
        """
        V1构建加密数据
        :param json_str:
        :param timestamp: 时间搓
        :param sign_nonce: 签名随机数
        :return:
        """
        sign_str = 'body={0}&timestamp={1}&signatureNonce={2}'.format(json_str, timestamp, sign_nonce)
        return sign_str


class V2(OpenApi):
    def structure_data(self, json_str, timestamp, sign_nonce):
        """
        V2构建加密数据
        :param json_str:
        :param timestamp: 时间搓
        :param sign_nonce: 签名随机数z
        :return:
        """
        return '{0}{1}{2}'.format(json_str, timestamp, sign_nonce)

    def build_headers(self, method, url, data=None):
        """
        V2构建回调 headers
        :param appsecret:
        :param body:
        :return:
        """
        headers = {
            'AccessKey': self.appkey,
            'Timestamp': str(int(round(time.time() * 1000))),
            'SignatureNonce': uuid.uuid4().hex,
        }

        if method.lower() in ['get', 'delete']:
            payload = url
        if method.lower() in ['post', 'put']:
            payload = json.dumps(data or {})

        sing = self.sign(headers, payload)
        headers['Signature'] = base64.b64encode(sing).decode()
        return headers


def get_api(version):
    if version == 'V1':
        return V1
    if version == 'V2':
        return V2
    assert False, '接口不支持的版本'
