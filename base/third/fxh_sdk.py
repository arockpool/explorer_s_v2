import uuid
import json
import logging
import time
import datetime
import requests
import traceback
import base64
from urllib import parse

from base.utils import debug


class FxhBase(object):

    def __init__(self):
        self.host = 'https://fxhapi.feixiaohao.com'

    def get_headers(self):
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'
        }

    def fetch(self, url, params={}, data={}):
        for i in range(5):
            time.sleep(0.5)
            try:
                logging.warning('url--> %s, params--> %s' % (url, params))
                result = requests.get(self.host + url, headers=self.get_headers(), params=params, data=data, timeout=100).json()
                # logging.warning('response--> %s' % result)
                return result
            except Exception as e:
                debug.get_debug_detail(e)
        return {}

    def get_ticker(self):
        '''
        获取交易行情
        '''
        url = '/public/v1/ticker'
        params = {'convert': 'cny'}
        return self.fetch(url=url, params=params)
