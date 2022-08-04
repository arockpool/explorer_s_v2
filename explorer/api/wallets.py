import json
from flask import request
from explorer.services.wallets import WalletsService
from base.response import response_json


def get_wallets_list():
    """
    获取钱包列表
    :return:
    """

    wallet_type = request.form.get('wallet_type')
    id_address_list = json.loads(request.form.get('id_address_list', '[]'))
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    result = WalletsService.get_wallets_list(wallet_type, id_address_list, page_index=page_index, page_size=page_size)
    return response_json(result)


def get_wallet_info():
    """
    获取钱包详情
    :return:
    """
    id_or_address = request.form.get('id_or_address')
    result = WalletsService.get_wallet_info(id_or_address)
    return response_json(result)


def get_wallet_record():
    """
    获取钱包记录
    :return:
    """
    id_or_address = request.form.get('id_or_address')
    result = WalletsService.get_wallet_record(id_or_address)
    return response_json(result)

