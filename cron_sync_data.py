# from gevent import monkey
# monkey.patch_all()
import os
import datetime
import logging
import requests
import threading
# import gevent
from functools import wraps
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.gevent import GeventScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from explorer.services.miner import MinerService
from explorer.services.overview import OverviewDayService
from explorer.services.message import TipsetGasService
from explorer.services.datacap import DataCapService
from explorer.services.deal import DealStatService
from app import create_app, init_web

app = create_app()
# init_web(app)

logging.basicConfig(
    format='%(levelname)s:%(asctime)s %(pathname)s--%(funcName)s--line %(lineno)d-----%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.WARNING
)

executors = {
    'default': ThreadPoolExecutor(10)
}


def func_log(func):
    '''
    记录操作信息
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):

        thread_id = threading.get_ident()
        start_time = datetime.datetime.now()
        logging.info('[== %s ==]开始执行方法[ %s ]' % (thread_id, func.__name__))
        try:
            result = func(*args, **kwargs)
            logging.warning(result)
        except Exception as e:
            logging.exception(e)
            result = {"code": 99904, "msg": "系统错误"}
        end_time = datetime.datetime.now()
        cost_time = end_time - start_time
        logging.info('[== %s ==]方法[ %s ]执行结束，耗时[ %s ]s' % (thread_id, func.__name__, cost_time.total_seconds()))

        return result

    return wrapper


@func_log
def sync_messages_stat():
    """同步消息分页的预先处理程序"""
    return TipsetGasService.sync_messages_stat()


@func_log
def sync_miner_total_blocks():
    '''同步活跃矿工区块区块信息'''
    return MinerService.sync_miner_total_blocks()


@func_log
def sync_miner_stat():
    '''同步矿工状态'''
    return MinerService.sync_miner_stat()


@func_log
def sync_miner_day():
    '''获取miner每天的数据'''
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return MinerService.sync_miner_day(yesterday_str)


@func_log
def sync_miner_day_block():
    '''获取miner每天的区块'''
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return MinerService.sync_miner_day_block(yesterday_str)


@func_log
def sync_miner_day_gas():
    '''同步矿工gas'''
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return MinerService.sync_miner_day_gas(yesterday_str)


@func_log
def sync_miner_pool():
    '''同步矿池概览'''
    return MinerService.save_miner_pool()


@func_log
def sync_overview_day():
    '''同步每天的全网概览'''
    # 需要依赖 TipsetGas
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return OverviewDayService.sync_overview_day(yesterday_str)


@func_log
def sync_overview_day_rmd():
    '''同步全网每天rmd产出数据'''
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    return OverviewDayService.sync_overview_day_rmd(yesterday_str)


@func_log
def sync_overview_stat():
    """
    同步每个高度的全网概览
    :return:
    """
    return OverviewDayService.sync_overview_stat()


@func_log
def sync_overview_tipset_gas():
    '''同步单个区块gas汇总'''
    # start_height = 1200000
    # eng_height = 1300000
    height = TipsetGasService.sync_overview_tipset_gas()
    logging.info("sync_overview_tipset_gas:{}".format(height))


@func_log
def sync_notaries():
    """同步公正人"""
    result = DataCapService.sync_notaries()
    logging.info("sync_notaries:{}".format(result))


@func_log
def sync_plus_client():
    """同步datacap客户"""
    result = DataCapService.sync_plus_client()
    logging.info("sync_plus_client:{}".format(result))


@func_log
def sync_add_verified_client():
    """同步datacap客户"""
    result = DataCapService.sync_add_verified_client()
    logging.info("sync_add_verified_client:{}".format(result))


@func_log
def sync_deal_provider():
    """同步datacap客户"""
    result = DataCapService.sync_deal_provider()
    logging.info("sync_deal_provider:{}".format(result))

@func_log
def sync_datacap_stats():
    """同步datacap客户"""
    result = DataCapService.sync_datacap_stats()
    logging.info("sync_datacap_stats:{}".format(result))


@func_log
def sync_messages_wallet():
    """处理消息中的钱包转账信息"""
    return TipsetGasService.sync_messages_wallet()


@func_log
def sync_deal_stat():
    """订单24小时统计"""
    result = DealStatService.sync_deal_stat()
    logging.info("sync_deal_stat:{}".format(result))


@func_log
def sync_deal_day():
    """订单每天的小时统计"""
    result = DealStatService.sync_deal_day()
    logging.info("sync_deal_day:{}".format(result))


@func_log
def sync_miner_lotus():
    """生态：脸上数据价格"""
    result = MinerService.sync_miner_lotus()
    logging.info("sync_miner_lotus:{}".format(result))

# @func_log
# def sync_miner_day_overtime_pledge_fee_by_last_7days():
#     # 同步之前7日浪费质押gas
#     ret = dict()
#     today = datetime.datetime.today()
#     for i in range(1, 8):
#         date = today - datetime.timedelta(days=i)
#         date = date.strftime('%Y-%m-%d')
#         request_dict = dict(date=date)
#         url = '%s/data/api/miner/sync_miner_day_overtime_pledge_fee' % os.getenv('SERVER_DATA')
#         resp = requests.post(url=url, timeout=600, data=request_dict).json()
#         ret[date] = resp
#     return ret


# @func_log
# def sync_overtime_pledge():
#     '''计算最近的过期质押'''
#     url = os.getenv('SERVER_DATA') + '/data/api/message/sync_overtime_pledge'
#     return requests.post(url=url, timeout=600, data={}).json()


if __name__ == '__main__':
    scheduler = BlockingScheduler(timezone="Asia/Shanghai", executors=executors)
    scheduler.add_job(func=sync_miner_total_blocks, trigger='cron', minute='*/10')
    scheduler.add_job(func=sync_miner_pool, trigger='cron', minute='*/40')
    scheduler.add_job(func=sync_messages_stat, trigger='cron', minute='*/4')
    scheduler.add_job(func=sync_miner_stat, trigger='cron', minute='*/10')
    scheduler.add_job(func=sync_miner_day, trigger='cron', hour=0, minute=10)
    scheduler.add_job(func=sync_miner_day_block, trigger='cron', hour=3, minute=10)
    scheduler.add_job(func=sync_miner_day_gas, trigger='cron', hour=3, minute=40)
    scheduler.add_job(func=sync_overview_day, trigger='cron', hour=0, minute=50)
    scheduler.add_job(func=sync_overview_day_rmd, trigger='cron', hour=10, minute=0)
    scheduler.add_job(func=sync_overview_stat, trigger='cron', minute='*/6')
    scheduler.add_job(func=sync_overview_tipset_gas, trigger='cron', minute='*/5')
    scheduler.add_job(func=sync_notaries, trigger='cron', minute='*/50')
    scheduler.add_job(func=sync_plus_client, trigger='cron', minute='*/59')
    scheduler.add_job(func=sync_add_verified_client, trigger='cron', minute='*/10')
    scheduler.add_job(func=sync_deal_provider, trigger='cron', minute='*/6')
    scheduler.add_job(func=sync_datacap_stats, trigger='cron', minute='*/30')
    scheduler.add_job(func=sync_messages_wallet, trigger='cron', minute='*/10')
    scheduler.add_job(func=sync_deal_stat, trigger='cron', minute='*/59')
    scheduler.add_job(func=sync_deal_day, trigger='cron', minute='*/30')
    scheduler.add_job(func=sync_miner_lotus, trigger='cron', minute='*/59')
    scheduler.start()
    # g.join()
