import os
import logging
import requests
import base64
from base.utils import debug


class BbheBase(object):
    def __init__(self):
        self.host = os.getenv('BBHEHOST')

    def get_headers(self):
        base64_secret = base64.b64encode(os.getenv('BBHESECRET').encode("utf-8")).decode("utf-8")
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
            "Authorization": 'Basic ' + base64_secret
        }

    def fetch(self, url, params={}, data={}):
        try:
            logging.warning('url--> %s, params--> %s' % (url, params))
            result = requests.get(self.host + url, headers=self.get_headers(), params=params, data=data, timeout=100).json()
            # logging.warning('response--> %s' % result)
            return result
        except Exception as e:
            debug.get_debug_detail(e)
            return {}

    def get_overview_stat(self, date):
        '''获取全网统计信息'''
        url = '/v1/data/asset/all/' + date
        return self.fetch(url=url, params={})

