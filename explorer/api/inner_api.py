import datetime
from flask import request
from explorer.services.message import MessageService
from explorer.services.miner import MinerService
from explorer.services.wallets import WalletsService
from explorer.services.blocks import BlocksService
from base.utils.fil import datetime_to_height
from base.response import response_json


def get_message_list_by_height():
    """
    获取Send消息列表根据高度
    :return:
    """

    height = request.form.get('height')
    result = MessageService.get_message_list_by_height(height)
    return response_json(result)


def get_miner_info_by_miner_no():
    """
    获取指定信息列表miner_no
    :return:
    """

    miner_no = request.form.get('miner_no')
    date_str = request.form.get('date_str')
    result = MinerService.get_miner_info_by_miner_no(miner_no, date_str)
    return response_json(result)


def get_wallet_address_change():
    """
    获取钱包指点数据大小的变化量
    :return:
    """

    wallet_address = request.form.get('wallet_address')
    balance_value = request.form.get('balance_value')
    height = int(request.form.get('height'))
    result = WalletsService.get_wallet_address_change(wallet_address, balance_value, height)
    return response_json(result)


def get_is_all_wallet():
    """
    查询是否是钱包
    :return:
    """
    address = request.form.get("address")
    if not address:
        return response_json(False)
    result = WalletsService.get_is_all_wallet(address)
    if result:
        return response_json(result["value"])
    return response_json(False)


def get_miner_day_list():
    """
    存储提供者每天的miner数据
    :return:
    """
    miner_no = request.form.get("miner_no")
    date = request.form.get("date")
    data = MinerService.get_miner_day_list(miner_no, date)
    return response_json(data)


def get_init_value():
    """
    存储提供者每天的miner数据
    :return:
    """
    miner_no = request.form.get("miner_no")
    fields = request.form.get("fields")
    end_time = request.form.get("end_time")
    data = MinerService.get_init_value(miner_no, fields, end_time)
    return response_json(data)


def get_block_count():
    """
    查询指定时间后是否还有新的区块
    :return:
    """
    date = request.form.get('date')
    height = datetime_to_height(date)
    count = BlocksService.get_tipset_block_count(start_height=height)
    return response_json({"count": count})


def get_miner_increment():
    """
    查询指定时间后是否还有新的区块
    :return:
    """
    miner_no = request.form.get('miner_no')
    date = request.form.get('date')
    key = request.form.get('key')
    if not date:
        date = str(datetime.date.today() - datetime.timedelta(days=1))
    result = MinerService.get_miner_increment(miner_no, date, key)
    return response_json(result)

