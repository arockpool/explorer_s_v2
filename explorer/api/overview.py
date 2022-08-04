import datetime
from flask import request
from explorer.api import util
# from explorer.api import migrate_es
from explorer.services.overview import OverviewDayService,OverviewService
from base.response import response_json


def sync_overview_day():
    """
    全网每天的数据
    :return:
    """

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return response_json(OverviewDayService.sync_overview_day())


def sync_overview_day_rmd():
    """
    全网每天的数据
    :return:
    """

    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday_str = yesterday.strftime('%Y-%m-%d')
    date_str = request.form.get('date_str')
    return response_json(OverviewDayService.sync_overview_day_rmd(date_str))


def sync_overview_stat():
    """
    全网个高度的数据
    :return:
    """
    return response_json(OverviewDayService.sync_overview_stat())


def get_overview():
    """
    获取全网概览
    :return:
    """
#     migrate_es.sync_all_overview
#     print(migrate_es.sync_all_overview)
    result = OverviewService.get_overview()
    return response_json(result)


def get_overview_stat():
    """
    获取全网统计数据
    :return:
    """
    result = OverviewService.get_overview_stat()
    return response_json(result)


def get_overview_day_list():
    """
    获取全网按天的数据列表
    :return:
    """
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    result = OverviewService.get_overview_day_list(start_date=start_date, end_date=end_date, page_index=page_index,
                                                   page_size=page_size)
    return response_json(result)


def get_overview_stat_list():
    """
    获取全网每个高度的数据
    :return:
    """
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    date = request.form.get('date')
    height = request.form.get('height')
    result = OverviewService.get_overview_stat_list(date=date, height=height, page_index=page_index,page_size=page_size)
    return response_json(result)


def get_overview_power_trends():
    """
    获取全网算力趋势图
    :return:
    """
    stats_type = request.form.get('stats_type', '30d')
    data = OverviewService.get_overview_power_trends(stats_type)
    return response_json(data)


def get_overview_stat_trends():
    """
    获取全网统计趋势图
    :return:
    """
    search_type = request.form.get('search_type', 'day')
    step, start_date, end_date = util.trends_parm(search_type)
    data = OverviewService.get_overview_stat_trends(start_date=start_date, end_date=end_date, step=step)
    return response_json(data)
