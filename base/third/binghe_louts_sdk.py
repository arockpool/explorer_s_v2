import base64
import logging
import requests
from requests.auth import HTTPBasicAuth
from base.utils import debug


class BbheLoutsBase(object):
    def __init__(self):
        self.host = "http://117.177.135.11:8490"

    @staticmethod
    def get_headers():
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
        }
        return headers

    @staticmethod
    def get_auth():
        return HTTPBasicAuth('bh', "fillotus")

    def fetch(self, url, params={}):
        try:
            logging.warning('url--> %s, params--> %s' % (url, params))
            result = requests.get(self.host + url, headers=self.get_headers(), auth=self.get_auth(), params=params,
                                  timeout=10).json()
            # logging.warning('response--> %s' % result)
            return result
        except Exception as e:
            debug.get_debug_detail(e)
            return {}

    def bill_to_miner_no(self, miner_id):
        path = '/lotus/miner/bill'
        params = {
            'miner_id': miner_id
        }
        return self.fetch(path, params)

