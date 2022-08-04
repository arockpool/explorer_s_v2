import datetime
from flask import request
from explorer.api import util
from explorer.services.blocks import BlocksService
from base.response import response_json


def get_tipsets():
    """
    获取块列表
    :return:
    """

    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 100)
    result = BlocksService.get_tipsets(page_index=page_index, page_size=page_size)
    return response_json(result)


def get_tipset_info():
    """
    获取区块高度详情
    :return:
    """
    height = int(request.form.get('height'))
    result = BlocksService.get_tipset_info(height)
    return response_json(result)


def get_block_info():
    """
    获取区块高度详情
    :return:
    """
    block_hash = request.form.get('block_hash')
    result = BlocksService.get_block_info(block_hash)
    return response_json(result)


def get_blocks():
    """
    获取区块列表
    :return:
    """
    miner_id = request.form.get('miner_id')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 2800)
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    result = BlocksService.get_blocks(miner_id, start_date, end_date,page_index,page_size)
    return response_json(result)