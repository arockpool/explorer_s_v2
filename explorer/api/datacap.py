import datetime
from flask import request
from explorer.api import util
from explorer.services.datacap import DataCapService
from base.response import response_json


def sync_notaries():
    """
    :return:
    """
    return response_json(DataCapService.sync_notaries())


def sync_plus_client():
    """
    :return:
    """
    return response_json(DataCapService.sync_plus_client())


def sync_add_verified_client():
    """
    :return:
    """
    return response_json(DataCapService.sync_add_verified_client())


def sync_deal_provider():
    """
    :return:
    """
    return response_json(DataCapService.sync_deal_provider())


def sync_datacap_stats():
    """
    :return:
    """
    date_str = request.form.get('date_str')
    return response_json(DataCapService.sync_datacap_stats(date_str))


def get_notaries_list():
    """
    获取公正人列表
    :return:
    """

    key_words = request.form.get('key_words')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 200)
    result = DataCapService.get_notaries_list(key_words, page_index=page_index, page_size=page_size)
    return response_json(result)


def get_plus_client_list():
    """
    获取客户列表
    :return:
    """

    key_words = request.form.get('key_words')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 200)
    result = DataCapService.get_plus_client_list(key_words, page_index=page_index, page_size=page_size)
    return response_json(result)


def get_provider_list():
    """
    获取存储供应商列表
    :return:
    """

    key_words = request.form.get('key_words')
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 10)), 200)
    result = DataCapService.get_provider_list(key_words, page_index=page_index, page_size=page_size)
    return response_json(result)


def get_datacap_dashboard():
    """
    获取钱包列表
    :return:
    """
    result = DataCapService.get_datacap_dashboard()
    return response_json(result)


def get_datacap_stats():
    """
    统计活跃度量
    :return:
    """
    stats_type = request.form.get('stats_type', "30d")
    result = DataCapService.get_datacap_stats(stats_type)
    return response_json(result)
