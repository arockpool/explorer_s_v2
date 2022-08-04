import datetime,json,math,decimal
import pandas as pd
from flask import request
from explorer.services.miner import MinerService
from base.response import response_json
from base.utils.fil import _d, bson_to_decimal, format_power, format_price,datetime_to_height
from explorer.services.message import MessageService
from explorer.services.blocks import BlocksService
from explorer.services.overview import OverviewService
from explorer.services.wallets import WalletsService


def sync_miner_total_blocks():
    """
    统计每个miner的出块汇总
    :return:
    """
    MinerService.sync_miner_total_blocks()
    return response_json(True)


def sync_miner_stat():
    """
    同步miner24小时信息
    :return:
    """
    MinerService.sync_miner_stat()
    return response_json(True)


def sync_miner_day():
    """
    获取miner每天的数据
    :return:
    """
    # datetime.date.today()
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday_str = yesterday.strftime('%Y-%m-%d')
    date_str = request.form.get("date_str")
    MinerService.sync_miner_day(date_str)
    return response_json(True)


def sync_miner_day_block():
    """
    获取miner每天的数据
    :return:
    """
    # datetime.date.today()
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday_str = yesterday.strftime('%Y-%m-%d')
    date_str = request.form.get("date_str")
    MinerService.sync_miner_day_block(date_str, reset=True)
    return response_json(True)


def sync_miner_day_gas():
    """
    统计miner——day的gas信息
    :return:
    """
    # date_str = request.form.get("date_str")
    # MinerService.sync_miner_day_gas(date_str)
    now_str = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    tables = pd.date_range('2021-09-13', now_str, freq='D').strftime("%Y-%m-%d").tolist()
    # tables.reverse()
    for date_str in tables:
        MinerService.sync_miner_day_gas(date_str, reset=True)
        print(date_str)
    return response_json(True)


def save_miner_pool():
    """
    同步存储池信息
    :return:
    """
    return response_json(MinerService.save_miner_pool())


def sync_miner_lotus():
    """
    链上价格和piece_size
    :return:
    """
    return response_json(MinerService.sync_miner_lotus())


def get_miners_by_address():
    """
    通过账户查询所属节点
    :return:
    """
    address = request.form.get("address")
    result = MinerService.get_miners_by_address(address)
    return response_json(result)


def get_miner_ranking_list():
    """
    首页排行榜
    :return:
    """
    stats_type = request.form.get("stats_type")
    sector_type = request.form.get("sector_type")
    filter_type = request.form.get("filter_type")
    miner_no_list = json.loads(request.form.get('miner_no_list', '[]'))
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    miner_no_dict, data = MinerService.get_miner_ranking_list(stats_type, filter_type, sector_type, miner_no_list,
                                                              page_index=page_index, page_size=page_size)
    miner_data = []
    for miner_day in data['objects']:
        miner_no = miner_day.get("miner_no") or miner_day["_id"]
        if "increase_power" == filter_type:
            tmp = dict(miner_no=miner_no, increase_power=bson_to_decimal(miner_day["increase_power"]),
                       increase_power_offset=bson_to_decimal(miner_day["increase_power_offset"]))
        if "avg_reward" == filter_type:
            tmp = dict(miner_no=miner_no, avg_reward=bson_to_decimal(miner_day["avg_reward"]))
        if "block_count" == filter_type:
            tmp = dict(miner_no=miner_no, win_count=miner_day["win_count"], lucky=bson_to_decimal(miner_day["lucky"],4),
                       block_reward=bson_to_decimal(miner_day["block_reward"]))
        tmp.update(miner_no_dict.get(miner_no))
        miner_data.append(tmp)
    data["objects"] = miner_data
    return response_json(data)


def get_miner_ranking_list_by_power():
    """
    首页算力排行榜
    :return:
    """
    sector_type = request.form.get("sector_type")
    miner_no_list = json.loads(request.form.get('miner_no_list', '[]'))
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    data = MinerService.get_miner_ranking_list_by_power(sector_type=sector_type, miner_no_list=miner_no_list,
                                                        page_index=page_index, page_size=page_size)
    return response_json(data)


def get_miner_pool_ranking_list():
    """
    首页存储池排行
    :return:
    """
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    data = MinerService.get_miner_pool_ranking_list(page_index=page_index, page_size=page_size)
    return response_json(data)


def get_miner_by_no():
    """
    存储提供者详情
    :return:
    """
    miner_no = request.form.get("miner_no")
    data = MinerService.get_miner_by_no(miner_no)
    return response_json(data)


def get_miner_stats_by_no():
    """
    存储提供者产出统计
    :return:
    """
    miner_no = request.form.get("miner_no")
    stats_type = request.form.get("stats_type", "24h")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    data = MinerService.get_miner_stats([miner_no], stats_type, start_date, end_date)
    return response_json(data)


def get_miner_gas_stats_by_no():
    """
    存储提供者成本统计
    :return:
    """
    miner_no = request.form.get("miner_no")
    stats_type = request.form.get("stats_type")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    data = MinerService.get_miner_gas_stats_by_no([miner_no], stats_type, start_date, end_date)
    return response_json(data)


def get_miner_line_chart_by_no():
    """
   存储提供者的算力变化和出块统计24/30/180
    :return:
    """
    miner_no = request.form.get("miner_no")
    stats_type = request.form.get("stats_type")
    data = MinerService.get_miner_line_chart_by_no([miner_no], stats_type)
    return response_json(data)


def get_miner_day_gas_list_by_no():
    """
   存储提供者每天的gas列表
    :return:
    """
    miner_no = request.form.get("miner_no")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    page_index = int(request.form.get('page_index', 1))
    page_size = min(int(request.form.get('page_size', 20)), 100)
    data = MinerService.get_miner_day_gas_list_by_no(miner_no, start_date, end_date, page_index=page_index,
                                                     page_size=page_size)
    return response_json(data)


def get_miner_pool_by_owner_id():
    """
    存储提供者详情
    :return:
    """
    owner_id = request.form.get("owner_id")
    data = MinerService.get_miner_pool_by_no(owner_id)
    return response_json(data)


def get_miner_pool_stats_by_owner_id():
    """
    存储pool产出统计
    :return:
    """
    owner_id = request.form.get("owner_id")
    stats_type = request.form.get("stats_type", "24h")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    data = MinerService.get_miner_pool_stats_by_owner_id(owner_id, stats_type, start_date, end_date)
    return response_json(data)


def get_miner_pool_gas_by_owner_id():
    """
    存储pool成本统计
    :return:
    """
    owner_id = request.form.get("owner_id")
    stats_type = request.form.get("stats_type", "24h")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    data = MinerService.get_miner_pool_gas_by_owner_id(owner_id, stats_type, start_date, end_date)
    return response_json(data)


def get_miner_pool_line_chart_by_owner_id():
    """
    存储存储pool提供者的算力变化和出块统计24/30/180
    :return:
    """
    owner_id = request.form.get("owner_id")
    stats_type = request.form.get("stats_type", "24h")
    data = MinerService.get_miner_pool_line_chart_by_owner_id(owner_id, stats_type)
    return response_json(data)


def get_miner_health_report_24h_by_no():
    """
    获取节点健康报告24小时数据
    """
    miner_no = request.form.get('miner_no')
    result = {}
    miner, miner_day_stat = MinerService.get_miner_stat_info(miner_no)
    if miner_day_stat:
        result["power"] = miner.actual_power
        result["sector_size"] = format_power(miner.sector_size, "GiB")
        result["avg_reward"] = miner_day_stat.avg_reward
        result["block_count"] = miner_day_stat.block_count
        result["block_reward"] = miner_day_stat.block_reward
        result["create_gas"], result["keep_gas"], result["pledge_gas"], result["total_gas"]\
            = MessageService.get_gas_cost_by_miner_no(miner_no)
        result["lucky"] = miner_day_stat.lucky
        end_height = datetime_to_height(datetime.datetime.now())
        overview_block_count = BlocksService.get_tipset_block_count(end_height-2880,end_height)
        # 爆快率 = 出块数/全网总出块数
        result["block_rate"] = format_price(miner_day_stat.block_count / overview_block_count, 4)
        worker = WalletsService.get_is_all_wallet(miner.worker_id)
        if worker:
            result["worker_balance"] = worker.value
        result["worker_balance"] = _d(0)
        poster = WalletsService.get_is_all_wallet(miner.post_id)
        if poster:
            result["poster_balance"] = poster.value
        result["poster_balance"] = _d(0)
        # result["worker_balance"] = miner.worker_balance
        # result["poster_balance"] = miner.poster_balance
        result["owner_address"] = miner.owner_address
        result["worker_address"] = miner.worker_address
        result["poster_address"] = miner.post_address
        result["total_sector"] = miner.sector_all
        result["active_sector"] = miner.sector_effect
        result["faulty_sector"] = miner.sector_faults
        result["recovering_sector"] = miner.sector_recovering
    return response_json(result)


def get_miner_health_report_day_by_no():
    """
    获取节点健康报告7天数组
    """
    result = []
    miner_no = request.form.get('miner_no')
    stat_type = request.form.get('stat_type', '7d')
    if stat_type == "7d":
        end_date = datetime.datetime.today()
        start_date = end_date - datetime.timedelta(days=7)
    objs = MinerService.get_miner_day_list_by_no(miner_no, start_date=start_date.strftime('%Y-%m-%d'),
                                                 end_date=end_date.strftime('%Y-%m-%d'))
    for obj in objs:
        result.append(dict(
            date=obj.date,
            avg_reward=obj.avg_reward,
            pledge_gas=obj.pledge_gas,
            lucky=obj.lucky,
            total_pledge=obj.pre_gas + obj.prove_gas + obj.win_post_gas,
            is_32=True if obj.sector_size == 34359738368 else False,
            create_gas=((obj.pre_gas+obj.prove_gas) / (obj.increase_power / _d(1024 ** 4))).quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP) if obj.increase_power else _d(0),
            keep_gas=(obj.win_post_gas / (obj.actual_power / _d(1024 ** 4))).quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP) if obj.actual_power else _d(0),
            worker_balance=obj.worker_balance,
            poster_balance=obj.post_balance
        ))
    return response_json(result)


def get_wallet_address_estimated_service_day():
    """
    钱包预计使用天数
    """
    # worker预计天数 = worker余额 / (节点近2日平均封装量 * (节点七天平均单T质押量 + 节点七天平均单T封装gas费))
    # post预计天数=post余额/(最近7天的矿池平均单T维护gas费*节点有效算力）
    miner_no = request.form.get('miner_no')
    miner = MinerService.get_miner_by_no(miner_no)
    end_date = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=7)
    miner_objs = MinerService.get_miner_day_list_by_no(miner_no, start_date=start_date.strftime('%Y-%m-%d'),
                                                       end_date=end_date.strftime('%Y-%m-%d'))
    increase_power_list = []  # 7天封装量
    create_gas_list = []  # 7天节点单体gas费
    win_gas_list = []  # 7天封装量
    for miner_obj in miner_objs:
        increase_power_list.append(miner_obj.increase_power)
        create_gas_list.append((miner_obj.pre_gas+miner_obj.prove_gas) / (miner_obj.increase_power / _d(1024 ** 4)) if miner_obj.increase_power else _d(0))
        win_gas_list.append((miner_obj.win_post_gas / miner_obj.actual_power) if miner_obj.actual_power else 0)
    increase_power_avg = sum(increase_power_list)/len(increase_power_list) / _d(math.pow(1024, 4))  # 节点近2日平均封装量;单位是T
    create_gas_avg = sum(create_gas_list)/len(create_gas_list)  # 节点七天平均单T质押量
    win_gas_avg = sum(win_gas_list)/len(win_gas_list)  # 单位是Bytes

    overview_objs = OverviewService._get_overview_day_list(start_date=start_date.strftime('%Y-%m-%d'),
                                                          end_date=end_date.strftime('%Y-%m-%d'))
    pledge_list = []  # 7天单T质押量
    for overview_obj in overview_objs:
        pledge_list.append(overview_obj.avg_pledge)
    pledge_avg = sum(pledge_list)/len(pledge_list) * _d(math.pow(10, 18))  # 节点七天平均单T质押量
    worker_estimated_service_day = -1
    poster_estimated_day= -1
    worker = WalletsService.get_is_all_wallet(miner.get("worker_id"))
    worker_balance = worker.value if worker else _d(0)
    poster = WalletsService.get_is_all_wallet(miner.get("post_id"))
    post_balance = poster.value if poster else _d(0)
    if increase_power_avg and worker_balance:
        worker_estimated_service_day = worker_balance // (increase_power_avg * (create_gas_avg+pledge_avg))
    if win_gas_avg and  miner.get("actual_power"):
        poster_estimated_day = post_balance // (win_gas_avg * miner.get("actual_power"))
    return response_json(data=dict(worker_estimated_day=worker_estimated_service_day,
                                   poster_estimated_day=poster_estimated_day))


def get_miner_health_report_gas_stat_by_no():
    miner_no = request.form.get('miner_no')
    stat_type = request.form.get('stat_type', '24h')
    result = MessageService.get_gas_cost_stat_by_miner_no(miner_no, stat_type)
    return response_json(data=result)


def get_messages_stat_by_miner_no():
    miner_no = request.form.get('miner_no')
    stat_type = request.form.get('stat_type', '24h')
    result = MessageService.get_messages_stat_by_miner_no(miner_no, stat_type)
    return response_json(data=result)
