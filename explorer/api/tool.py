from flask import request
from explorer.services.deal import DealService
from base.response import response_json
from explorer.services.wallets import WalletsService
from explorer.services.tool import WalletDirectionService


def get_is_wallet():
    """
    查询是否是钱包
    :return:
    """
    address = request.form.get("address")
    if not address:
        return response_json(False)
    result = WalletsService.get_is_wallet(address)
    if result:
        return response_json(True)
    return response_json(False)


def get_wallet_flow_list():
    """
    获取钱包流向消息
    :return:
    """
    address = request.form.get('address')
    direction = request.form.get('direction')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    result = WalletDirectionService.get_wallet_flow_list(address, direction, page_index, page_size)
    return response_json(result)


def get_wallet_message_list():
    """
    获取钱包流向详情
    :return:
    """
    from_address = request.form.get('from_address')
    to_address = request.form.get('to_address')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    result = WalletDirectionService.get_wallet_message_list(from_address, to_address, page_index, page_size)
    return response_json(result)


