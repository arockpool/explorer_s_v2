import datetime
from explorer.api import util
from flask import request
from explorer.services.message import TipsetGasService, TipsetService, MessageService
from base.response import response_json


def sync_tipset_gas():
    """
    同步单个区块的全网gas汇总
    :return:
    """

    end_index = request.form.get('end_index')
    start_index = request.form.get('start_index')
    TipsetGasService.sync_overview_tipset_gas()
    return response_json(True)


def sync_messages_stat():
    """

    :return:
    """
    TipsetGasService.sync_messages_stat()
    return response_json(True)


def sync_messages_wallet():
    """

    :return:
    """
    return response_json(TipsetGasService.sync_messages_wallet())


def get_gas_trends():
    """
    获取gas费用趋势图
    :return:
    """
    search_type = request.form.get('search_type', 'day')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    step, start_date, end_date = util.trends_parm(search_type, start_date, end_date)
    data = TipsetService.get_gas_trends(start_date=start_date, end_date=end_date, step=step)
    return response_json(data)


def get_gas_stat_all():
    """
    获取gas24小时统计
    :return:
    """
    data = TipsetService.get_gas_stat_all()
    return response_json(data)


def get_message_list():
    miner_id = request.form.get('miner_id')
    block_hash = request.form.get('block_hash')
    msg_method = request.form.get('msg_method')
    msgrct_exit_code = request.form.get('msgrct_exit_code')
    height = request.form.get('height')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    data = MessageService.get_message_list(miner_id, block_hash, msg_method, msgrct_exit_code, height,
                                           page_index=page_index, page_size=page_size)
    return response_json(data)


def get_message_method_list():
    """
    获取方法详情
    :return:
    """
    miner_id = request.form.get('miner_id')
    block_hash = request.form.get('block_hash')
    data = MessageService.get_message_method_list(miner_id, block_hash)
    return response_json(data)


def get_message_info():
    """
    获取消息详情
    :return:
    """
    msg_cid = request.form.get('msg_cid')
    height = request.form.get('height')
    result = MessageService.get_message_info(msg_cid, height)
    return response_json(result)


def get_mpool_list():
    key_words = request.form.get('key_words')
    msg_method = request.form.get('msg_method')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 100)
    data = MessageService.get_mpool_list(key_words, msg_method, page_index=page_index, page_size=page_size)
    return response_json(data)


def get_mpool_info():
    cid = request.form.get('cid')
    result = MessageService.get_mpool_info(cid)
    return response_json(result)


def get_transfer_list():
    """
    获取转账列表
    :return:
    """
    msg_method = request.form.get('msg_method')
    miner_id = request.form.get('miner_id')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    page_size = int(request.form.get('page_size', 20))
    page_index = int(request.form.get('page_index', 1))
    data = MessageService.get_message_list(miner_id, msg_method=msg_method, start_date=start_date, end_date=end_date,
                                           is_transfer=True, page_index=page_index, page_size=page_size)
    return response_json(data)


def get_transfer_method_list():
    """
    获取转账列表方法详情
    :return:
    """
    miner_id = request.form.get('miner_id')
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    data = MessageService.get_message_method_list(miner_id, start_date=start_date, end_date=end_date,
                                                  is_transfer=True)
    return response_json(data)
