import datetime
from flask import request
from base.response import response_json
from base.third.gateio_sdk import GateioBase
from explorer.services.message import MessageService
from explorer.services.blocks import BlocksService
from explorer.services.miner import MinerService
from explorer.services.wallets import WalletsService


def get_price():
    """
    获取价格和价格变化量
    :return:
    """
    result = {"price": '0', "price_change": '0', "24h_low": '0', "24h_high": '0'}
    gateio_result = GateioBase().get_ticker()
    if gateio_result:
        result["price"] = gateio_result.get('last', '0')
        result["price_change"] = gateio_result.get('percentChange', '0')
        result["low"] = gateio_result.get('low24hr', '0')
        result["high"] = gateio_result.get('high24hr', '0')
    return response_json(result)


def search():
    """
    搜索
    """
    # 判断是否中文,如果是中文就直接返回
    value = request.form.get("value")
    for str_data in value:
        if '\u4e00' <= str_data <= '\u9fff':
            return
    # 判断是否为矿工
    result = MinerService.get_is_miner(value)
    if result:
        return response_json({"address": value, "type": "address"})
    result = WalletsService.get_is_wallet(value)
    if result:
        return response_json({"address": value, "type": "address"})
    # 判断是否为区块高度
    if value.isdigit():
        result = BlocksService.get_is_tipset(value)
        if result:
            return response_json({"address": value, "type": "tipset"})
    # 判断是否为区块id
    result = BlocksService.get_is_block(value)
    if result:
        return response_json({"address": value, "type": "block"})
    # 判断是否为消息id
    result = MessageService.get_is_message(value)
    if result:
        return response_json({"address": value, "type": "message", "height": result["height"]})
    return response_json({})


def search_miner_or_wallet():
    """
    判断是否是矿工还是钱包(前端遗留问问题)
    """
    value = request.form.get("value")
    # 判断是否为矿工
    result = MinerService.get_is_miner(value)
    if result:
        return response_json({"address": value, "type": "shot"})
    # 判断是否为钱包
    result = WalletsService.get_is_wallet(value)
    if result:
        return response_json({"address": result.address, "type": "wallet"})
    return response_json({})


def search_miner_type():
    """
    查询存储供应商相关的类型数据(前端遗留问问题)
    """
    # post/worker/owner/miner
    value = request.form.get("value")
    return response_json(MinerService.search_miner_type(value))
