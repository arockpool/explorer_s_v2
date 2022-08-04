import datetime
from flask import request
from explorer.api import util
from explorer.services.deal import DealService, DealStatService
from base.response import response_json


def get_deal_list():
    """
    获取钱包列表
    :return:
    """

    key_words = request.form.get('key_words')
    start_time_height = request.form.get('start_time_height')
    end_time_height = request.form.get('end_time_height')
    is_verified = int(request.form.get('is_verified', 2))
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 100)
    result = DealService.get_deal_list(key_words, is_verified, start_time_height, end_time_height,
                                       page_index=page_index, page_size=page_size)
    return response_json(result)


def get_deal_info():
    """
    获取钱包详情
    :return:
    """
    deal_id = request.form.get('deal_id')
    result = DealService.get_deal_info(deal_id)
    return response_json(result)


def get_deal_info_list():
    """
    已经生效的订单池子
    :return:
    """

    key_words = request.form.get('key_words')
    start_time_height = request.form.get('start_time_height')
    end_time_height = request.form.get('end_time_height')
    is_verified = int(request.form.get('is_verified', 2))
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 2880)
    result = DealService.get_deal_info_list(key_words, is_verified, start_time_height, end_time_height,
                                            page_index=page_index, page_size=page_size)
    return response_json(result)


def sync_deal_stat():
    """
    24小时数据统计
    :return:
    """
    return response_json(DealStatService.sync_deal_stat())


def sync_deal_day():
    """
    每天数据统计
    :return:
    """
    date_str = request.form.get('date_str')
    # flag = True
    # while flag:
    #     result = DealStatService.sync_deal_day()
    #     print(result)
    #     if not result:
    #         flag = False
    return response_json(str(DealStatService.sync_deal_day(date_str)))


def get_deal_stat():
    """
    订单24统计
    :return:
    """
    result = DealStatService.get_deal_stat()
    return response_json(result)


def get_deal_day():
    """
    订单每天数据统计
    :return:
    """
    stats_type = request.form.get('stats_type', "30d")
    result = DealStatService.get_deal_day(stats_type)
    return response_json(result)
