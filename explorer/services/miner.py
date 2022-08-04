import datetime, math
import decimal
import json

import pandas as pd
from flask import request
from pymongo import UpdateOne
from mongoengine.connection import get_db
from base.utils.paginator import mongo_paginator, mongo_aggregate_paginator
from base.utils.fil import _d, bson_to_decimal, datetime_to_height, get_aggregate_gas, local2utc, utc2local, \
    bson_dict_to_decimal
from explorer.models.miner import Miners, MinerStat, MinerHotHistory, MinerDay, MinerSyncLog, MinersPool
from explorer.models.overview import Overview, OverviewDay
from explorer.models.message import Messages, TipsetGas
from explorer.services.blocks import BlocksService, Blocks
from explorer.services.wallets import WalletsService
from mongoengine import Q
from bson.decimal128 import Decimal128
from base.flask_ext import cache
from base.third import bbhe_louts_sdk


class MinerService(object):
    """
    矿工服务
    """

    @classmethod
    def sync_miner_stat(cls):
        """
        同步miner24小时信息
        :return:
        """
        miner_stats = []
        miner_historys = []
        over_view = Overview.objects().order_by("-height").only("height", "power").first()
        end_height = over_view.height - 200  # 修补block需要150个高度
        start_height = end_height - 2880
        data = BlocksService.get_blocks_by_range_height(start_height=start_height,
                                                        end_height=end_height)
        data_dict = {d["_id"]: d for d in data}
        for miner in Miners.objects(sector_effect__ne=0).no_dereference().all():
            miner_history, miner_stat = cls.get_miner_hot_history(miner, data_dict.get(miner.miner, {}),
                                                                  over_view.power)
            miner_stats.append(miner_stat)
            miner_historys.append(miner_history)
        modified_count=0
        if miner_stats:
            modified_count = get_db("business").miner_stat.bulk_write(miner_stats).modified_count
        if miner_historys:
            get_db("business").miner_hot_history.bulk_write(miner_historys)
        return modified_count

    @classmethod
    def get_24h_power_increase(cls, miner):
        """
        获取24小时算力增加量
        :param miner:
        :return:
        """
        record_time = datetime.datetime.now()
        last_time = record_time - datetime.timedelta(days=1)
        last_record = MinerHotHistory.objects(miner_no=miner.miner,
                                              record_time__lte=local2utc(last_time)).order_by("-record_time").first()
        if not last_record:
            return _d(miner.sector_all * miner.sector_size), miner.actual_power
        # 计算算力增速、算力增量
        increase_power = (miner.sector_all - last_record.sector_all) * miner.sector_size
        increase_power_offset = miner.actual_power - last_record.actual_power
        return _d(increase_power), increase_power_offset

    @classmethod
    def get_miner_hot_history(cls, miner, data, power):
        """添加矿工48小时热表数据"""
        now = datetime.datetime.now()
        minute = math.floor(now.minute / 30) * 30
        record_time = datetime.datetime(now.year, now.month, now.day, now.hour, minute, 0)
        miner_history = UpdateOne({"miner_no": miner.miner, "record_time": local2utc(record_time)},
                                  {"$set": dict(
                                      raw_power=Decimal128(miner.raw_power),
                                      actual_power=Decimal128(miner.actual_power),
                                      sector_all=miner.sector_all,
                                      sector_effect=miner.sector_effect,
                                      sector_size=miner.sector_size
                                  )},
                                  upsert=True
                                  )
        if data:
            pass
        block_reward = bson_to_decimal(data.get("sum_gas_reward", Decimal128("0"))) + \
                       bson_to_decimal(data.get("sum_block_reward", Decimal128("0")))
        win_count = data.get("sum_win_count", 0)
        block_count = data.get("block_count", 0)
        # 计算平均收益
        avg_reward = block_reward / (miner.actual_power / _d(1024 ** 4)) if miner.actual_power else 0
        avg_reward = avg_reward.quantize(decimal.Decimal("0"), rounding=decimal.ROUND_HALF_UP)
        # 计算24小时算力增速、增量
        increase_power, increase_power_offset = cls.get_24h_power_increase(miner)
        lucky = round(block_count / cls._get_24h_theory_block_count(miner.actual_power, power), 4)
        miner_stat = UpdateOne({"miner_no": miner.miner},
                               {"$set": dict(
                                   block_reward=Decimal128(block_reward),
                                   block_count=block_count,
                                   win_count=win_count,
                                   increase_power=Decimal128(increase_power),
                                   increase_power_offset=Decimal128(increase_power_offset),
                                   avg_reward=Decimal128(avg_reward),
                                   lucky=Decimal128(_d(lucky)),
                                   sector_size=miner.sector_size
                               )},
                               upsert=True)

        return miner_history, miner_stat

    @classmethod
    def _get_24h_theory_block_count(cls, actual_power, power):
        """
        或者24h理论出块数量
        :param actual_power:
        :param power:
        :return:
        """
        return actual_power / power * 2880 * 5

    @classmethod
    def sync_miner_day(cls, date_str=None):
        """
        按天统计miner
        :return:
        """
        if not date_str:
            date = datetime.date.today() - datetime.timedelta(days=1)
            date_str = date.strftime("%Y-%m-%d")
        miner_days = []
        for miner in Miners.objects(sector_effect__ne=0).no_dereference().all():
            miner_day = cls.get_miner_day(miner, date_str)
            miner_days.append(miner_day)
        get_db("business").miner_day.bulk_write(miner_days)

    @classmethod
    def get_miner_day(cls, miner, date_str):
        """
        获取miner每天的数据
        :param miner:
        :param date_str:
        :return:
        """
        # 获取上一次的记录，用于计算增量
        last_record = MinerDay.objects(miner_no=miner.miner, date__lt=date_str).order_by("-date").first()
        # 新增扇区
        new_sector = miner.sector_all
        if last_record:
            new_sector = miner.sector_all - (last_record.sector_all or 0)
        # 新增算力
        increase_power = _d(new_sector * miner.sector_size)
        # 新增算力增量
        last_actual_power = _d(0)
        if last_record:
            last_actual_power = last_record.actual_power
        increase_power_offset = miner.actual_power - last_actual_power
        worker = WalletsService.get_is_all_wallet(miner.worker_id)
        worker_balance = worker.value if worker else _d(0)
        owner = WalletsService.get_is_all_wallet(miner.owner_id)
        owner_balance = owner.value if owner else _d(0)
        poster = WalletsService.get_is_all_wallet(miner.post_id)
        post_balance = poster.value if poster else _d(0)
        miner_day = UpdateOne({"miner_no": miner.miner, "date": date_str},
                              {"$set": dict(
                                  actual_power=Decimal128(miner.actual_power),
                                  raw_power=Decimal128(miner.raw_power),
                                  sector_size=miner.sector_size,
                                  sector_all=miner.sector_all,
                                  sector_effect=miner.sector_effect,
                                  total_balance=Decimal128(miner.total_balance),
                                  available_balance=Decimal128(miner.available_balance),
                                  precommit_deposits_balance=Decimal128(miner.precommit_deposits_balance or "0"),
                                  initial_pledge_balance=Decimal128(miner.initial_pledge_balance),
                                  locked_balance=Decimal128(miner.locked_balance),
                                  total_reward=Decimal128(miner.total_reward or "0"),
                                  total_block_count=miner.total_block_count or 0,
                                  total_win_count=miner.total_win_count or 0,
                                  increase_power=Decimal128(increase_power),
                                  increase_power_offset=Decimal128(increase_power_offset),
                                  block_reward=Decimal128("0"),
                                  avg_reward=Decimal128("0"),
                                  win_count=0,
                                  block_count=0,
                                  lucky=Decimal128("0"),
                                  pre_gas=Decimal128("0"),
                                  pre_gas_count=0,
                                  prove_gas=Decimal128("0"),
                                  prove_gas_count=0,
                                  win_post_gas=Decimal128("0"),
                                  win_post_gas_count=0,
                                  pledge_gas=Decimal128("0"),
                                  worker_balance=Decimal128(worker_balance),
                                  post_balance=Decimal128(post_balance),
                                  owner_balance=Decimal128(owner_balance),
                              )},
                              upsert=True)
        return miner_day

    @classmethod
    def sync_miner_total_blocks(cls, ):
        """
        统计每个miner的出块汇总
        :return:
        """
        data = BlocksService.get_blocks_by_range_height()
        miners = []
        for per in data:
            total_reward = bson_to_decimal(per.get("sum_gas_reward", Decimal128("0"))) + \
                           bson_to_decimal(per.get("sum_block_reward", Decimal128("0")))
            miners.append(UpdateOne({"miner": per.get("_id")},
                                    {"$set": dict(
                                        total_reward=Decimal128(total_reward),
                                        total_win_count=per.get("sum_win_count", 0),
                                        total_block_count=per.get("block_count", 0))
                                    }))
        result = get_db("base").miners.bulk_write(miners)
        return result.modified_count

    @classmethod
    def sync_miner_day_block(cls, date_str, reset=False):
        """
        获取miner每天的区块数据
        :param date_str:
        :param reset:
        :return:
        """
        if date_str:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            date = datetime.date.today() - datetime.timedelta(days=1)
            date_str = date.strftime("%Y-%m-%d")
        start_height = datetime_to_height(date)
        end_height = start_height + 2880
        block_data = BlocksService.get_blocks_by_range_height(start_height=start_height, end_height=end_height)
        data_dict = {d["_id"]: d for d in block_data}
        over_view = Overview.objects(height__lt=end_height).order_by("-height").only("height", "power").first()

        miner_days = MinerDay.objects(date=date_str).all()
        miner_day_requests = []
        for miner_day in miner_days:
            if not reset:
                miner = Miners.objects(miner=miner_day.miner_no).first()
                actual_power = max(miner.actual_power, miner_day.actual_power)
            else:
                actual_power = miner_day.actual_power
            data = data_dict.get(miner_day.miner_no, {})
            block_reward = bson_to_decimal(data.get("sum_gas_reward", Decimal128("0"))) + \
                           bson_to_decimal(data.get("sum_block_reward", Decimal128("0")))
            win_count = data.get("sum_win_count", 0)
            block_count = data.get("block_count", 0)
            # 计算平均收益
            avg_reward = block_reward / (actual_power / _d(1024 ** 4)) if actual_power else 0
            avg_reward = avg_reward.quantize(decimal.Decimal("0"), rounding=decimal.ROUND_HALF_UP)
            lucky = round(block_count / cls._get_24h_theory_block_count(actual_power, over_view.power), 4)
            miner_day_requests.append(UpdateOne({"miner_no": miner_day.miner_no, "date": date_str},
                                                {"$set": dict(
                                                    block_reward=Decimal128(block_reward),
                                                    avg_reward=Decimal128(avg_reward),
                                                    win_count=win_count,
                                                    block_count=block_count,
                                                    lucky=Decimal128(_d(lucky)),
                                                )}))
        get_db("business").miner_day.bulk_write(miner_day_requests)

    @classmethod
    def sync_miner_day_gas(cls, date_str=None, reset=False):
        """
        统计miner——day的gas信息
        :param date_str:
        :param reset:
        :return:
        """
        if not date_str:
            date = datetime.date.today() - datetime.timedelta(days=1)
            date_str = date.strftime("%Y-%m-%d")
        save_per_count = 200
        search_step = 5
        sync_obj = MinerSyncLog.objects(date=date_str).first()

        # 开始时间戳
        start_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        start_index = datetime_to_height(start_date)
        temp_index = max(start_index, sync_obj.gas_sync_height if sync_obj else 0)
        # 结束时间戳
        end_index = start_index + 2880
        # 如果需要重置
        if reset:
            # 重置进度表
            sync_obj.gas_sync_height = start_index
            sync_obj.save()
            temp_index = start_index
            # 重置每日矿工表
            MinerDay.objects(date=date_str).update(pre_gas=0, prove_gas=0, win_post_gas=0, pre_gas_count=0,
                                                   prove_gas_count=0, win_post_gas_count=0, pledge_gas=0)

        def _add_v(d, k, t, v, ps, s_c=1):
            if k not in d:
                d[k] = {
                    'pre_gas': _d(0), 'prove_gas': _d(0), 'win_post_gas': _d(0), 'pledge_gas': _d(0),
                    'pre_gas_count': 0, 'prove_gas_count': 0, 'win_post_gas_count': 0
                }
            d[k][t] += bson_to_decimal(v)
            d[k]["pledge_gas"] += bson_to_decimal(ps)
            d[k][t + '_count'] += s_c

        miner_gas_dict = {}
        while temp_index < end_index:
            heights = [x for x in range(temp_index, temp_index + search_step)]
            messages = get_db("base")["messages@zone_" + start_date.strftime("%Y%m")].find({"height": {"$in": heights}})
            # with switch_collection(Messages, "messages@zone_" + start_date.strftime("%Y%m")) as Message_s:
            #     heights = [x for x in range(temp_index, temp_index + search_step)]
            #     messages = Message_s.objects(height__in=heights).as_pymongo().all()
            for per in messages:
                miner_no = per['msg_to']
                # SubmitWindowedPoSt
                if per['msg_method'] == 5:
                    _add_v(miner_gas_dict, miner_no, 'win_post_gas', per['gascost_total_cost'], Decimal128("0"))
                # PreCommitSector PreCommitSectorBatch
                if per['msg_method'] in [6, 25]:
                    pre_agg_gas = 0
                    sector_count = per.get("sector_count", 1)
                    if per.get('msgrct_exit_code') == 0:
                        pre_agg_gas = get_aggregate_gas(sector_count, int(per["base_fee"]), per['height'],
                                                        per['msg_method'])
                    _add_v(miner_gas_dict, miner_no, 'pre_gas',
                           Decimal128(bson_to_decimal(per['gascost_total_cost']) + _d(pre_agg_gas)),
                           Decimal128("0"), sector_count)
                # ProveCommitSector ProveCommitAggregate
                if per['msg_method'] in [7, 26]:
                    prove_agg_gas = 0
                    pledge_value = Decimal128("0")
                    sector_count = per.get("sector_count", 1)
                    if per.get('msgrct_exit_code') == 0:
                        pledge_value = per['msg_value']
                        prove_agg_gas = get_aggregate_gas(sector_count, int(per["base_fee"]), per['height'],
                                                          per['msg_method'])
                    _add_v(miner_gas_dict, miner_no, 'prove_gas',
                           Decimal128(bson_to_decimal(per['gascost_total_cost']) + _d(prove_agg_gas)),
                           pledge_value, sector_count)

            temp_index += search_step
            # 每隔save_per_count次保存一次
            if temp_index % save_per_count == 0:
                cls.save_miner_gas(data=miner_gas_dict, date=date_str, height=temp_index)
                miner_gas_dict = {}
        # 收尾
        if miner_gas_dict:
            cls.save_miner_gas(data=miner_gas_dict, date=date_str, height=temp_index)

    @classmethod
    def save_miner_gas(cls, data, date, height):
        """
        保存汽油费
        :param data:
        :param date:
        :param height:
        :return:
        """
        miner_days = []
        for miner_no, one_data in data.items():
            miner_days.append(
                UpdateOne({"miner_no": miner_no, "date": date},
                          {"$inc": {"pre_gas": Decimal128(one_data["pre_gas"]),
                                    "pre_gas_count": one_data["pre_gas_count"],
                                    "prove_gas": Decimal128(one_data["prove_gas"]),
                                    "prove_gas_count": one_data["prove_gas_count"],
                                    "win_post_gas": Decimal128(one_data["win_post_gas"]),
                                    "win_post_gas_count": one_data["win_post_gas_count"],
                                    "pledge_gas": Decimal128(one_data["pledge_gas"])}})
            )
        if miner_days:
            get_db("business").miner_day.bulk_write(miner_days)
        MinerSyncLog.objects(date=date).upsert_one(gas_sync_height=height)

    @classmethod
    def save_miner_pool(cls):
        """
        活跃存储池
        :return:
        """
        miner_pool_list = []
        pipeline = [{"$group": {"_id": "$owner_address",
                                "miner_list": {"$addToSet": "$miner"},
                                "total_balance": {"$sum": "$total_balance"},
                                "available_balance": {"$sum": "$available_balance"},
                                "initial_pledge_balance": {"$sum": "$initial_pledge_balance"},
                                "locked_balance": {"$sum": "$locked_balance"},
                                "actual_power": {"$sum": "$actual_power"},
                                "raw_power": {"$sum": "$raw_power"},
                                "sector_all": {"$sum": "$sector_all"},
                                "sector_effect": {"$sum": "$sector_effect"},
                                "sector_faults": {"$sum": "$sector_faults"},
                                "sector_recovering": {"$sum": "$sector_recovering"},
                                "total_reward": {"$sum": "$total_reward"},
                                "total_block_count": {"$sum": "$total_block_count"},
                                "total_win_count": {"$sum": "$total_win_count"},
                                }}]
        for owner_id in Miners.objects(sector_effect__ne=0).distinct("owner_id"):
            miners = list(Miners.objects(owner_id=owner_id).aggregate(pipeline))
            if not miners:
                continue
            miner_pool = UpdateOne({"owner_id": owner_id},
                                   {"$set": dict(
                                       owner_address=miners[0].get("_id"),
                                       miner_list=miners[0].get("miner_list", []),
                                       total_balance=miners[0].get("total_balance"),
                                       available_balance=miners[0].get("available_balance"),
                                       initial_pledge_balance=miners[0].get("initial_pledge_balance"),
                                       locked_balance=miners[0].get("locked_balance"),
                                       actual_power=miners[0].get("actual_power"),
                                       raw_power=miners[0].get("raw_power"),
                                       sector_all=miners[0].get("sector_all"),
                                       sector_effect=miners[0].get("sector_effect"),
                                       sector_faults=miners[0].get("sector_faults"),
                                       sector_recovering=miners[0].get("sector_recovering"),
                                       total_reward=miners[0].get("total_reward"),
                                       total_block_count=miners[0].get("total_block_count"),
                                       total_win_count=miners[0].get("total_win_count"),
                                   )}, upsert=True)
            miner_pool_list.append(miner_pool)
        modified_count = 0
        if miner_pool_list:
            modified_count = get_db("business").miners_pool.bulk_write(miner_pool_list).modified_count
        return modified_count

    @classmethod
    def sync_miner_lotus(cls):
        """
        链上价格和piece_size
        :return:
        """
        historys = []
        record_time = datetime.datetime.now()
        for miner in Miners.objects(actual_power__gt=Decimal128("0")).no_dereference().all():
            result_louts = bbhe_louts_sdk.BbheLoutsBase().bill_to_miner_no(miner.miner)
            if result_louts.get("code") == 0:
                data = result_louts.get("data")
                max_piece_size = Decimal128(data.get("max_piece_size", "0"))
                min_piece_size = Decimal128(data.get("min_piece_size", "0"))
                try:
                    price = Decimal128(data.get("price", "0"))
                    verified_price = Decimal128(data.get("verified_price", "0"))
                except Exception:
                    price = Decimal128("0")
                    verified_price = Decimal128("0")
                historys.append(UpdateOne({"miner_no": miner.miner, "record_time": local2utc(record_time)},
                                          {"$set": dict(
                                              max_piece_size=max_piece_size,
                                              min_piece_size=min_piece_size,
                                              price=price,
                                              verified_price=verified_price,)
                                          }, upsert=True)
                                )
        modified_count = 0
        if historys:
            modified_count = get_db("business").miner_deal_price_history.bulk_write(historys).modified_count
        return modified_count

    @classmethod
    def get_increase_power_loss(cls):
        """
        获取算力损失
        :return:
        """
        # 32
        sector_size_32 = 34359738368
        faults_power_32 = Miners.objects(sector_size=sector_size_32).sum("sector_faults") * _d(sector_size_32)
        sector_size_64 = 68719476736
        faults_power_64 = Miners.objects(sector_size=sector_size_64).sum("sector_faults") * _d(sector_size_64)
        return faults_power_32 + faults_power_64

    @classmethod
    def get_is_miner(cls, value):
        """
        判断是否是miner
        :param value:
        :return:
        """
        return Miners.objects(Q(miner=value) | Q(address=value)).first()

    @classmethod
    def get_miners_by_address(cls, address):
        """
        获取算力损失
        :return:
        """
        result = dict()
        # 名下节点
        result["subordinate"] = list(Miners.objects(owner_address=address).scalar("miner"))
        # 工作节点
        result["worker"] = list(Miners.objects(Q(worker_address=address) | Q(post_address=address)).scalar("miner"))

        return result

    @classmethod
    def get_miner_stat_ranking_list(cls, order=None, sector_type=None, miner_no_list=[]):
        """
        获取24小时的矿工排名记录
        :param order:
        :param sector_type:
        :param miner_no_list:
        :return:
        """
        query_dict = {}
        if sector_type is not None:
            if sector_type == '0':
                query_dict["sector_size"] = 34359738368
            if sector_type == '1':
                query_dict["sector_size"] = 68719476736
        if miner_no_list:
            query_dict["miner_no__in"] = miner_no_list
        query = MinerStat.objects(**query_dict)
        if order:
            query = query.order_by(order)

        return query.as_pymongo()

    @classmethod
    def get_miner_day_ranking_list(cls, start_date, end_date, sector_type=None, miner_no_list=[],
                                   filter_type="increase_power", page_index=1, page_size=20):
        """
        获取指定日期的矿工排行榜数据
        :param start_date:
        :param end_date:
        :param sector_type:
        :param miner_no_list:
        :param filter_type:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {"date__gte": start_date, "date__lte": end_date}
        if sector_type is not None:
            if sector_type == '0':
                query_dict["sector_size"] = 34359738368
            if sector_type == '1':
                query_dict["sector_size"] = 68719476736
        if miner_no_list:
            query_dict["miner_no__in"] = miner_no_list
        if filter_type == "increase_power":
            pipeline = [
                {"$group": {"_id": "$miner_no",
                            "increase_power": {"$avg": "$increase_power"},
                            "increase_power_offset": {"$avg": "$increase_power_offset"}
                            }},
                {"$sort": {"increase_power": -1}}
            ]
        if filter_type == "avg_reward":
            query_dict["avg_reward__lt"] = 10 ** 18
            pipeline = [
                {"$group": {"_id": "$miner_no",
                            "avg_reward": {"$avg": "$avg_reward"}
                            }},
                {"$sort": {"avg_reward": -1}}
            ]
        if filter_type == "block_count":
            pipeline = [
                {"$group": {"_id": "$miner_no",
                            "win_count": {"$sum": "$win_count"},
                            "lucky": {"$avg": "$lucky"},
                            "block_reward": {"$sum": "$block_reward"},
                            }},
                {"$sort": {"win_count": -1}},
            ]
        return mongo_aggregate_paginator(MinerDay.objects(**query_dict), pipeline, page_index, page_size)

    @classmethod
    def get_miner_ranking_list(cls, stats_type, filter_type, sector_type=None, miner_no_list=[],
                               page_index=1, page_size=20):
        """
        矿工排行子函数
        :param stats_type:
        :param filter_type:
        :param sector_type:
        :param miner_no_list:
        :param page_index:
        :param page_size:
        :return:
        """
        if stats_type == "24h":
            query = cls.get_miner_stat_ranking_list("-" + filter_type, sector_type, miner_no_list)
            data = mongo_paginator(query, page_index, page_size)
            miner_no_list = [info["miner_no"] for info in data['objects']]
            if "block_count" == filter_type:
                data["total_block_reward"] = cls.get_miner_stat_total_block_reward(sector_type)
        else:
            end_date = datetime.date.today() - datetime.timedelta(days=1)
            start_date = end_date - datetime.timedelta(days=int(stats_type[0:stats_type.find("d")]))
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")
            data = cls.get_miner_day_ranking_list(start_date, end_date,
                                                  sector_type, miner_no_list, filter_type, page_index, page_size)
            miner_no_list = [info["_id"] for info in data['objects']]
            if "block_count" == filter_type:
                data["total_block_reward"] = cls.get_miner_day_total_block_reward(start_date, end_date, sector_type)
        miner_list = Miners.objects(miner__in=miner_no_list).all()
        miner_no_dict = {}
        for miner in miner_list:
            tmp = {"power": miner.actual_power, "sector_size": miner.sector_size}
            miner_no_dict[miner.miner] = tmp
        return miner_no_dict, data

    @classmethod
    def get_miner_stat_total_block_reward(cls, sector_type=None):
        """
        获取指定日期的矿工排行榜数据
        :param sector_type:
        :return:
        """
        query_dict = {}
        if sector_type is not None:
            if sector_type == '0':
                query_dict["sector_size"] = 34359738368
            if sector_type == '1':
                query_dict["sector_size"] = 68719476736
        return bson_to_decimal(MinerStat.objects(**query_dict).sum("block_reward"))

    @classmethod
    def get_miner_day_total_block_reward(cls, start_date, end_date, sector_type=None):
        """
        获取指定日期的矿工排行榜数据
        :param start_date:
        :param end_date:
        :param sector_type:
        :return:
        """
        query_dict = {"date__gte": start_date, "date__lte": end_date}
        if sector_type is not None:
            if sector_type == '0':
                query_dict["sector_size"] = 34359738368
            if sector_type == '1':
                query_dict["sector_size"] = 68719476736
        return bson_to_decimal(MinerDay.objects(**query_dict).sum("block_reward"))

    @classmethod
    def get_miner_ranking_list_by_power(cls, sector_type=None, miner_no_list=[], page_index=1, page_size=20):
        """
        存储节点算力排行
        :param sector_type:
        :param miner_no_list:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {"sector_effect__ne": 0}
        if sector_type is not None:
            if sector_type == '0':
                query_dict["sector_size"] = 34359738368
            if sector_type == '1':
                query_dict["sector_size"] = 68719476736
        if miner_no_list:
            query_dict["miner__in"] = miner_no_list

        query = Miners.objects(**query_dict).order_by("-actual_power")
        data = mongo_paginator(query, page_index, page_size)
        miner_no_list = [info["miner"] for info in data['objects']]
        miner_stat_list = MinerStat.objects(miner_no__in=miner_no_list).all()
        miner_stat_dict = {miner_stat.miner_no: miner_stat.to_dict() for miner_stat in miner_stat_list}
        miner_data = []
        for miner in data['objects']:
            miner_no = miner.miner
            tmp = dict(miner_no=miner_no, power=miner.actual_power, )
            tmp.update(miner_stat_dict.get(miner_no))
            miner_data.append(tmp)
        data["objects"] = miner_data
        return data

    @classmethod
    @cache.cached(timeout=600, key_prefix=lambda: "view/get_miner_pool_ranking_list:%s}" % json.dumps(request.form))
    def get_miner_pool_ranking_list(cls, page_index=1, page_size=20):
        """
        存储池算力排行
        :param page_index:
        :param page_size:
        :return:
        """
        query = MinersPool.objects().order_by("-actual_power")
        data = mongo_paginator(query, page_index, page_size)
        miner_data = []
        for miner_pool in data['objects']:
            miner_stat_dict = cls.get_miner_pool_stats_by_owner_id(miner_pool.owner_id, "24h")
            tmp = dict(owner_id=miner_pool.owner_id, power=miner_pool.actual_power,
                       total_block_count=miner_pool.total_block_count, avg_reward=miner_stat_dict.get("avg_reward"),
                       increase_power_offset=miner_stat_dict.get("increase_power_offset"))
            miner_data.append(tmp)
        data["objects"] = miner_data
        return data

    @classmethod
    def get_miner_by_no(cls, miner_no):
        """
        根据矿工no获取信息
        :return:
        """
        miner = Miners.objects(Q(miner=miner_no) | Q(address=miner_no)).first()
        data = miner.to_dict()
        ranking = Miners.objects(actual_power__gt=miner.actual_power).count() + 1
        data["ranking"] = ranking
        # data["miner_id"] = miner.miner
        return data

    @classmethod
    def get_miner_stats(cls, miner_no_list, stats_type, start_date=None, end_date=None):
        """
        矿工详情展示产出统计
        """
        now_date = datetime.datetime.today()
        groups = {"_id": 0,
                  "increase_power": {"$sum": "$increase_power"},
                  "increase_power_offset": {"$sum": "$increase_power_offset"},
                  "block_reward": {"$sum": "$block_reward"},
                  "block_count": {"$sum": "$block_count"},
                  "win_count": {"$sum": "$win_count"},
                  "lucky": {"$avg": "$lucky"},
                  }

        def _format_data(_start_date, _end_date=None):
            query_dict = {"miner_no__in": miner_no_list}
            if _start_date:
                query_dict["date__gte"] = _start_date
            if _end_date:
                query_dict["date__lte"] = _end_date
            groups["actual_power"] = {"$avg": "$actual_power"}
            _objs = MinerDay.objects(**query_dict).aggregate([{"$group": groups}])
            _data = list(_objs)
            result_dict = _data[0] if _data else {}
            _lucky = bson_to_decimal(result_dict.get("lucky", 0), 4)
            result_dict = bson_dict_to_decimal(result_dict)
            result_dict["lucky"] = _lucky
            if result_dict["actual_power"]:
                result_dict["avg_reward"] = (result_dict["block_reward"] / (
                        result_dict["actual_power"] / _d(math.pow(1024, 4)))).quantize(decimal.Decimal("1"),
                                                                                       decimal.ROUND_HALF_UP)
            else:
                result_dict["avg_reward"] = _d(0)
            return result_dict

        _result = dict(avg_reward=0,
                       lucky=0,
                       increase_power=0,
                       increase_power_offset=0,
                       block_reward=0,
                       block_count=0,
                       win_count=0)
        result = {}
        if stats_type == "7d":
            _start_date = now_date - datetime.timedelta(days=7)
            result = _format_data(_start_date.strftime("%Y-%m-%d"))
        if stats_type == "30d":
            _start_date = now_date - datetime.timedelta(days=30)
            result = _format_data(_start_date.strftime("%Y-%m-%d"))
        if stats_type == "24h":
            objs = MinerStat.objects(miner_no__in=miner_no_list).aggregate([{"$group": groups}])
            data = list(objs)
            result = data[0] if data else {}
            lucky = bson_to_decimal(result["lucky"], 4)
            result = bson_dict_to_decimal(result)
            result["lucky"] = lucky
            actual_power = bson_to_decimal(Miners.objects(miner__in=miner_no_list).sum("actual_power"))
            if actual_power:
                result["avg_reward"] = (result["block_reward"] / (
                        actual_power / _d(math.pow(1024, 4)))).quantize(decimal.Decimal("1"),
                                                                        decimal.ROUND_HALF_UP)
            else:
                result["avg_reward"] = _d(0)
        if start_date and end_date:
            result = _format_data(start_date, end_date)
        return result or _result

    @classmethod
    def get_miner_pool_by_no(cls, owner_id):
        """
        存储pool详情
        :return:
        """
        miner = MinersPool.objects(Q(owner_id=owner_id) | Q(owner_address=owner_id)).first()
        data = miner.to_dict()
        ranking = MinersPool.objects(actual_power__gt=miner.actual_power).count() + 1
        data["ranking"] = ranking
        # data["miner_id"] = miner.miner
        return data

    @classmethod
    def get_miner_pool_stats_by_owner_id(cls, owner_id, stats_type, start_date=None, end_date=None):
        """
        存储pool详情展示产出统计
        :return:
        """
        miner_pool = MinersPool.objects(owner_id=owner_id).first()
        return cls.get_miner_stats(miner_pool.miner_list, stats_type, start_date, end_date)

    @classmethod
    def get_miner_gas_stats_by_no(cls, miner_no_list, stats_type, start_date=None, end_date=None):
        """
        矿工详情展示成本统计
        """
        now_date = datetime.datetime.today()

        def _format_data(_start_date, _end_date=None):
            # win_post_gas = _d(0)  # wingas
            # create_total_gas = _d(0)  # 生产gas
            query_dict = {"miner_no__in": miner_no_list}
            if _start_date:
                query_dict["date__gte"] = _start_date
            if _end_date:
                query_dict["date__lte"] = _end_date

            groups = {"_id": 0,
                      "increase_power": {"$sum": "$increase_power"},
                      "pre_gas": {"$sum": "$pre_gas"},
                      "prove_gas": {"$sum": "$prove_gas"},
                      "win_post_gas": {"$sum": "$win_post_gas"},
                      "total_pledge": {"$sum": "$pledge_gas"},
                      }
            objs = MinerDay.objects(**query_dict).aggregate([{"$group": groups}])
            _data = list(objs)
            _result_dict = bson_dict_to_decimal(_data[0]) if _data else {}

            # objs = MinerDay.objects(**query_dict).order_by("-date")
            # if not objs:
            #     return {}
            # power = objs[0].actual_power
            # sector_size = objs[0].sector_size
            # initial_pledge_balance = objs[0].initial_pledge_balance
            # last_initial_pledge_balance = 0  #
            # count = 0
            # last_power = 0
            # for obj in objs:
            #     count += 1
            #     create_total_gas += obj.pre_gas + obj.prove_gas
            #     win_post_gas += obj.win_post_gas
            #     last_initial_pledge_balance = obj.initial_pledge_balance
            #     last_power = obj.actual_power
            # total_pledge = initial_pledge_balance - last_initial_pledge_balance  # 质押
            # 生产成本
            create_total_gas = _d(_result_dict.get("pre_gas", 0) + _result_dict.get("prove_gas", 0))
            create_gas = _d(0)
            increase_power = _result_dict.get("increase_power", 0)
            if increase_power:
                create_gas = create_total_gas / (increase_power / _d(1024 ** 4))
                create_gas = create_gas.quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP)
            # 全网生产成本
            start_height = datetime_to_height(_start_date + " 00:00:00")
            query_dict["height__gte"] = start_height
            if _end_date:
                end_height = datetime_to_height(_end_date + " 23:59:59")
                query_dict["height__lte"] = end_height
            groups_gas = {"_id": 0,
                          "create_gas_32": {"$avg": "$create_gas_32"},
                          "create_gas_64": {"$avg": "$create_gas_64"},
                          }
            gas_ = list(TipsetGas.objects(height__gte=start_height).aggregate([{"$group": groups_gas}]))
            gas_dict = bson_dict_to_decimal(gas_[0]) if gas_ else {}
            create_gas_overview = (gas_dict.get("create_gas_32", 0) + gas_dict.get("create_gas_64", 0)) / 2
            result_dict = dict(total_gas=create_total_gas + _d(_result_dict.get("win_post_gas", 0)),
                               total_pledge=_result_dict.get("total_pledge", 0),
                               create_gas=create_gas, create_gas_overview=create_gas_overview)
            return result_dict

        if stats_type:
            days = int(stats_type[:-1])
            _start_date = now_date - datetime.timedelta(days=days)
            return _format_data(_start_date.strftime("%Y-%m-%d"))
        if start_date and end_date:
            return _format_data(start_date, end_date)
        return {}

    @classmethod
    def get_miner_pool_gas_by_owner_id(cls, owner_id, stats_type, start_date=None, end_date=None):
        """
        存储pool详情展示gas统计
        :return:
        """
        miner_pool = MinersPool.objects(owner_id=owner_id).first()
        return cls.get_miner_gas_stats_by_no(miner_pool.miner_list, stats_type, start_date, end_date)

    @classmethod
    def _get_miner_line_chart_by_no(cls, miner_no_list, days, spot=30):
        step = int(days / spot)
        now_date = datetime.datetime.today()
        ds_day = now_date - datetime.timedelta(days=days)
        ds_day_list = [ds_day + datetime.timedelta(days=step * x) for x in range(0, spot)]
        pipeline = [
            {"$group": {"_id": "$date",
                        "actual_power": {"$sum": "$actual_power"},
                        "increase_power_offset": {"$sum": "$increase_power_offset"},
                        "block_reward": {"$sum": "$block_reward"},
                        "block_count": {"$sum": "$block_count"}
                        }}
        ]
        objs = MinerDay.objects(miner_no__in=miner_no_list, date__gte=ds_day.strftime('%Y-%m-%d')).aggregate(
            pipeline)
        ds_day_dict = {}
        for obj in objs:
            ds_day_dict[obj["_id"]] = {
                "power": bson_to_decimal(obj["actual_power"]),
                "increase_power_offset": bson_to_decimal(obj["increase_power_offset"]),
                "block_reward": bson_to_decimal(obj["block_reward"]),
                "block_count": obj["block_count"],
                "date": obj["_id"]
            }
        ds_day_list.reverse()
        ds_day_result = []
        for day_step in ds_day_list:
            if step > 1:
                power = ds_day_dict.get(day_step.strftime('%Y-%m-%d'), {}).get('power', 0)
                increase_power_offset = _d(0)
                block_reward = _d(0)
                block_count = _d(0)
                for day1 in [day_step - datetime.timedelta(days=x) for x in range(0, step)]:
                    day_ = day1.strftime('%Y-%m-%d')
                    increase_power_offset += ds_day_dict.get(day_, {}).get('increase_power_offset', _d(0))
                    block_reward += ds_day_dict.get(day_, {}).get('block_reward', _d(0))
                    block_count += ds_day_dict.get(day_, {}).get('block_count', _d(0))
                ds_day_result.append({
                    "power": power,
                    "increase_power_offset": increase_power_offset,
                    "block_reward": block_reward,
                    "block_count": block_count,
                    "date": day_step.strftime('%Y-%m-%d')
                })
            else:
                ds_day_result.append(ds_day_dict.get(day_step.strftime('%Y-%m-%d'), {
                    "power": _d(0),
                    "increase_power_offset": _d(0),
                    "block_reward": _d(0),
                    "block_count": _d(0),
                    "date": day_step.strftime('%Y-%m-%d')
                }))
        return ds_day_result

    @classmethod
    def get_miner_line_chart_by_no(cls, miner_no_list, stats_type):
        """
        矿工的算力变化和出块统计
        """
        if stats_type == "30d":
            return cls._get_miner_line_chart_by_no(miner_no_list, 30, 30)
        if stats_type == "180d":
            return cls._get_miner_line_chart_by_no(miner_no_list, 180, 30)
        if stats_type == "24h":
            hs_24 = datetime.datetime.now() - datetime.timedelta(days=1)
            hs_24_list = [(hs_24 + datetime.timedelta(hours=1 * x)).replace(minute=0, second=0, microsecond=0) for x in
                          range(0, 24)]
            pipeline = [
                {"$project": {"block_reward": "$block_reward",
                              "date": {"$dateToString": {"format": "%Y-%m-%d %H", "date": "$height_time"}}}},
                {"$group": {"_id": "$date",
                            "block_reward": {"$sum": "$block_reward"},
                            "block_count": {"$sum": 1}
                            }}
            ]
            data = Blocks.objects(miner_id__in=miner_no_list, height__gte=datetime_to_height(hs_24)).aggregate(
                pipeline)
            hs_24_dict = {}
            for obj in data:
                date = utc2local(datetime.datetime.strptime(obj["_id"], "%Y-%m-%d %H"))
                hs_24_dict[date] = {
                    "block_reward": bson_to_decimal(obj["block_reward"]),
                    "block_count": obj["block_count"],
                    "date": date.strftime("%Y-%m-%d %H:%M:%S")
                }
            hs_24_result = []
            for hs1 in hs_24_list:
                hs_24_result.append(hs_24_dict.get(hs1, {
                    "block_reward": 0,
                    "block_count": 0,
                    "date": hs1.strftime('%Y-%m-%d %H:%M:%S')
                }))
            hs_24_result.reverse()
            return hs_24_result

    @classmethod
    def get_miner_pool_line_chart_by_owner_id(cls, owner_id, stats_type):
        """
        存储池的算力变化和出块统计
        :param owner_id:
        :param stats_type:
        :return:
        """
        miner_pool = MinersPool.objects(owner_id=owner_id).first()
        return cls.get_miner_line_chart_by_no(miner_pool.miner_list, stats_type)

    @classmethod
    def get_miner_day_gas_list_by_no(cls, miner_no, start_date=None, end_date=None, page_index=1, page_size=20):
        query_dict = dict(miner_no=miner_no)
        if start_date and end_date:
            query_dict["date__gte"] = start_date
            query_dict["date__lte"] = end_date
        result = mongo_paginator(MinerDay.objects(**query_dict).order_by("-date"), page_index, page_size)
        # 矿工每天的数据
        result_list = []
        for value in result['objects']:
            create_total_gas = value.pre_gas + value.prove_gas + value.overtime_pledge_fee
            win_post_gas = value.win_post_gas
            increase_power = value.increase_power
            create_gas = _d(0)
            if increase_power:
                create_gas = create_total_gas / (increase_power / _d(1024 ** 4))
                create_gas = create_gas.quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP)
            win_gas = _d(0)
            if value.actual_power:
                win_gas = win_post_gas / (value.actual_power / _d(1024 ** 4))
                win_gas = win_gas.quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP)
            result_list.append(dict(
                date=value.date,
                increase_power=increase_power,
                increase_power_offset=value.increase_power_offset,
                create_total_gas=create_total_gas,
                pledge_gas=value.pledge_gas,
                win_post_gas=win_post_gas,
                total_gas=create_total_gas + win_post_gas,
                create_gas=create_gas,
                win_gas=win_gas
            ))
        result['objects'] = result_list
        return result

    @classmethod
    def get_miner_info_by_miner_no(cls, miner_no, date_str=None):
        """
        获取指定信息
        :param miner_no:
        :param date_str:
        :return:
        """
        if not date_str:
            miner = Miners.objects(miner=miner_no).first()
            if not miner:
                return {}
            return miner.to_dict(only_fields=["actual_power", "sector_size", "total_reward", "total_balance",
                                              "available_balance", "locked_balance", "initial_pledge_balance",
                                              "precommit_deposits_balance", "address"])
        miner_day = MinerDay.objects(miner_no=miner_no, date=date_str).first()
        if not miner_day:
            return {}
        data = miner_day.to_dict(only_fields=["actual_power", "increase_power_offset", "increase_power",
                                              "block_reward", "avg_reward", "sector_size",
                                              "total_reward", "total_balance", "available_balance", "locked_balance",
                                              "initial_pledge_balance", "date"])

        def _avg_T(total_gas):
            avg_gas = _d(0)
            increase_power = miner_day.increase_power
            if increase_power:
                avg_gas = total_gas / (increase_power / _d(1024 ** 4))
                avg_gas = avg_gas.quantize(decimal.Decimal("1"), decimal.ROUND_HALF_UP)
            return avg_gas

        # 生产Gas
        create_total_gas = miner_day.pre_gas + miner_day.prove_gas
        data["create_gas"] = _avg_T(create_total_gas)
        # 维护gas
        data["win_gas"] = (miner_day.win_post_gas / (miner_day.actual_power / _d(1024 ** 4))).quantize(
            decimal.Decimal("1"),
            decimal.ROUND_HALF_UP)
        # 全网质押
        data["avg_pledge_gas"] = _d(0)
        overview_day = OverviewDay.objects(date=date_str).first()
        if overview_day:
            data["avg_pledge_gas"] = overview_day.avg_pledge
            if data["create_gas"] == _d(0):
                if miner_day.sector_size == 68719476736:
                    data["create_gas"] = overview_day.create_gas_64
                if miner_day.sector_size == 34359738368:
                    data["create_gas"] = overview_day.create_gas_32
        return data

    @classmethod
    def search_miner_type(cls, value):
        miner = Miners.objects(
            Q(miner=value) | Q(address=value) | Q(owner_id=value) | Q(owner_address=value) | Q(worker_id=value) | Q(
                worker_address=value) | Q(post_id=value) | Q(post_address=value)).first()
        if not miner:
            return {}
        fields = ["miner", "address", "owner_id", "worker_id", "owner_address", "post_address",
                  "worker_address", "post_id"]
        for field in fields:
            if getattr(miner, field) == value:
                return {"type": field, field: value}

    @classmethod
    def get_miner_stat_info(cls, miner_no):
        miner = Miners.objects(miner=miner_no).first()
        if miner:
            return miner, MinerStat.objects(miner_no=miner_no).first()
        return None, None

    @classmethod
    def get_miner_day_list_by_no(cls, miner_no, start_date=None, end_date=None):
        return MinerDay.objects(miner_no=miner_no, date__gte=start_date, date__lte=end_date).order_by("-date").all()

    @classmethod
    def get_miner_day_list(cls, miner_no, date):
        """
        那天获取miner每天的数据
        :param miner_no:
        :param date:
        :return:
        """
        miner_day = MinerDay.objects(miner_no=miner_no, date=date).first()
        return miner_day.to_dict()

    @classmethod
    def get_init_value(cls, miner_no, fields, end_time):
        objs = MinerDay.objects(date__lt=end_time, miner_no=miner_no)
        field_list = fields.split(",")
        result_dict = {}
        for field in field_list:
            if field == "create_gas":  # 生产gas
                data = objs.aggregate([{"$group": {"_id": 0, "pre_gas_sum": {"$sum": "$pre_gas"},
                                                   "prove_gas_sum": {"$sum": "$prove_gas"}}},
                                       {"$project": {"create_gas": {"$add": ["$pre_gas_sum", "$prove_gas_sum"]}}}])
                data = list(data)
                result = bson_to_decimal(data[0].get("create_gas")) if data else 0
            elif field == "initial_pledge_balance":
                if not objs:
                    result = 0
                else:
                    result = objs.order_by("-date").first().initial_pledge_balance
            else:
                result = bson_to_decimal(objs.sum(field))
            result_dict[field] = result
        return result_dict

    @classmethod
    def get_miner_increment(cls, miner_no, date, key=None):
        """
        获取该日的增量
        """
        before_day = str((datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=1)).date())
        forget_day_data = MinerDay.objects(miner_no=miner_no, date=date).first()
        before_day_data = MinerDay.objects(miner_no=miner_no, date=before_day).first()
        if not forget_day_data or not before_day_data:
            return {}
        if key:
            fields_data = [key]
        else:
            fields_data = forget_day_data._data
        before_day_data.to_dict()
        result_dict = dict()
        for field in fields_data:
            if isinstance(eval("forget_day_data.{}".format(field)), int) or isinstance(
                    eval("forget_day_data.{}".format(field)), decimal.Decimal):
                result_dict[field] = eval("forget_day_data.{}".format(field)) - eval("before_day_data.{}".format(field))
        return result_dict
