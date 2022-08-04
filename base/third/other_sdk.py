"""
其余第三方sdk
"""
import decimal
import logging
import os

import requests
import json
from base.utils import debug
import time
import hashlib


def fetch(url, headers={}, params={}, data={}, json={}, method='get'):
    try:
        logging.warning('url--> %s, json --> %s' % (url, json))
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'}
        if method == 'get':
            result = requests.get(url, headers=headers, params=params, data=data, json=json, timeout=30).json()
        elif method == "post":
            result = requests.post(url, headers=headers, params=params, data=data, json=json, timeout=30).json()
        else:
            return {}
        # logging.warning('response--> %s' % result)
        return result
    except Exception as e:
        debug.get_debug_detail(e)
        return {}


def get_coin_detail():
    """
    查询单T当日币价
    :return:
    """
    url = r'https://api.rmdine.com/open/stat/solomine/data'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['code'] == 0:
        unit_price = decimal.Decimal(content['data'])

        return unit_price
    else:
        return None


def get_gas_generate_detail():
    url = r'https://api.rmdine.com/open/stat/solomine/data'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['code'] == 0:
        unit_price = decimal.Decimal(1)
        return unit_price
    else:
        return None


def get_gas_maintain_detail():
    url = r'https://api.rmdine.com/open/stat/solomine/data'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['code'] == 0:
        unit_price = decimal.Decimal(1)
        return unit_price
    else:
        return None


def get_pledge_detail():
    # 扇区质押
    url = r'https://api.rmdine.com/open/stat/solomine/data'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['code'] == 0:
        unit_price = decimal.Decimal(1)
        return unit_price
    else:
        return None


def gas_detail():
    # 计算gas费
    url = r'https://filfox.info/api/v1/stats/message/fee'
    result = fetch(url)
    if not result or result.get("statusCode"):
        # 异常处理
        pass
    else:
        # 维护gas费,生成gas费
        maintain, generate = 0, 0
        for method_dict in result:
            # 封存gas费
            if method_dict['method'] == "ProveCommitSector":
                pass
            elif method_dict['method'] == "PreCommitSector":
                pass
            # 维护gas费
            elif method_dict['method'] == "SubmitWindowedPoSt":
                pass
        return maintain, generate


def get_usd_rate():
    url = 'http://api.k780.com/?app=finance.rate&scur=USD&tcur=CNY&appkey=10003&sign=b59bc3ef6191eb9f747dd4e83c99f2a4'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['success'] == "1":
        unit_price = decimal.Decimal(content['result']['rate'])
        return unit_price
    else:
        return decimal.Decimal(6.5)  # 查询失败返回汇率固定值


def get_mean_earnings():
    url = r'https://api.rmdine.com/open/stat/solomine/data'
    response = requests.get(url)
    content = json.loads(response.content.decode())
    if content['code'] == 0:
        unit_price = decimal.Decimal(content.get('data'))
        return unit_price
    else:
        return None


class RMDine():
    def __init__(self):
        self.host = os.getenv('RR_HOST')
        self.timestamp = int(time.time())
        self.merchant_id = os.getenv('RR_FIL_MERCHANT_ID')
        self.merchant_key = os.getenv('RR_MERCHANT_KEY')

    def get_sign(self):
        sign_str = "merchant_id={merchant_id}&timestamp={timestamp}{merchant_key}".format(
            merchant_id=self.merchant_id, timestamp=self.timestamp, merchant_key=self.merchant_key)
        print(sign_str)
        sign = hashlib.md5((sign_str).encode('utf-8')).hexdigest()
        return sign

    def get_fil_overview(self):
        url = "{}/ipfs/fil_overview".format(self.host)
        data = {
            "timestamp": self.timestamp,
            "merchant_id": self.merchant_id,
            "sign": self.get_sign()
        }
        return fetch(url=url, data=data, method='post')
