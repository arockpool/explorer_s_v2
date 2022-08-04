import datetime, math
import pandas as pd
from collections import OrderedDict
from base.utils.fil import _d, get_aggregate_gas, height_to_datetime, datetime_to_height, local2utc, \
    utc2local, bson_to_decimal, bson_dict_to_decimal
from explorer.models.message import Messages, TipsetGas, TipsetGasStat, Mpool, MessagesStat, \
    MessagesStatLog, MessagesStatInfo
from explorer.models.blocks import Blocks, BlockMessages, Tipset
from explorer.models.wallets import WalletFlow, WalletFlowLog
from mongoengine import Q
from mongoengine.connection import get_db
from pymongo import UpdateOne
from base.utils.paginator import mongo_paginator
from bson.decimal128 import Decimal128


class TipsetGasService(object):
    """
    全网gas数据服务
    """

    @classmethod
    def sync_overview_tipset_gas(cls, ):
        """
        同步单个区块的全网gas汇总
        :return:
        """
        height = 1
        # height = start_height
        # query_dict_stat = {"height__gte": start_height}
        # if end_height:
        #     query_dict_stat["height__lte"] = end_height
        over_view_stat = TipsetGas.objects().order_by("-height").first()
        if over_view_stat:
            height = over_view_stat.height
        # end_height = height + 200
        now_height = datetime_to_height(datetime.datetime.now()) - 60  # 统计降低个高度避免可能分叉等导致数据错误
        end_height = min(height + 201, now_height)
        # over_views = Overview.objects(height__gt=height).order_by("height").no_dereference().limit(2880)
        #
        # launch_date = datetime.datetime(2020, 8, 25, 6, 0, 0)
        # if not end_index:
        #     end_index = int((datetime.datetime.now() - launch_date).total_seconds() / 30)
        # end_index = int(end_index)
        #
        # if not start_index:
        #     start_index = end_index - 121
        # start_index = int(start_index)
        # 同步新的数据
        for i in range(height + 1, end_height):
            TipsetGasService.sync_tipset_gas(height=i)
            TipsetGasService.sync_tipset_gas_stat(height=i)
        # 消息延迟预警
        # MessageBase().sync_tipset_gas_warning()
        return end_height

    @classmethod
    def sync_tipset_gas(cls, height):
        """同步汽油费相关"""
        height_datetime = height_to_datetime(height)
        table_name = height_datetime.strftime("%Y%m")
        messages = get_db("base")["messages@zone_" + table_name].find({"height": height})
        # with switch_collection(Messages, "messages@zone_" + height_datetime.strftime("%Y%m")) as Message_s:
        #     messages = Message_s.objects(height=height).no_dereference().all()
        if messages:
            cls._sync_tipset_gas(height=height, messages=messages)

    @classmethod
    def _sync_tipset_gas(cls, height, messages):
        '''同步单个区块gas汇总'''
        pre_gas_32 = _d(0)
        pre_gas_32_count = 0
        pre_gas_64 = _d(0)
        pre_gas_64_count = 0
        prove_gas_32 = _d(0)
        prove_gas_32_count = 0
        prove_gas_64 = _d(0)
        prove_gas_64_count = 0
        winpost_gas_32 = _d(0)
        winpost_gas_32_count = 0
        winpost_gas_64 = _d(0)
        winpost_gas_64_count = 0

        base_fee = 0
        is_flag = False
        for message in messages:
            is_flag = True
            if not base_fee:
                base_fee = message.get("base_fee")
            # SubmitWindowedPoSt
            if message.get("msg_method") == 5:
                if message.get("sector_size_value") == 34359738368:
                    winpost_gas_32 += bson_to_decimal(message.get("gascost_total_cost"))
                    winpost_gas_32_count += 1
                else:
                    winpost_gas_64 += bson_to_decimal(message.get("gascost_total_cost"))
                    winpost_gas_64_count += 1
            # PreCommitSector PreCommitSectorBatch
            if message.get("msg_method") in [6, 25]:
                sector_count = message.get("sector_count", 1)
                aggregate_gas = 0
                if message.get('msgrct_exit_code') == 0:
                    aggregate_gas = _d(get_aggregate_gas(message.get("sector_count", 0), message.get("base_fee", 0),
                                                         height, message.get("msg_method")))
                if message.get("sector_size_value") == 34359738368:
                    pre_gas_32 += bson_to_decimal(message.get("gascost_total_cost")) + aggregate_gas
                    pre_gas_32_count += sector_count
                else:
                    pre_gas_64 += bson_to_decimal(message.get("gascost_total_cost")) + aggregate_gas
                    pre_gas_64_count += sector_count
            # ProveCommitSector ProveCommitAggregate
            if message.get("msg_method") in [7, 26]:
                sector_count = message.get("sector_count", 1)
                aggregate_gas = 0
                if message.get('msgrct_exit_code') == 0:
                    aggregate_gas = _d(get_aggregate_gas(message.get("sector_count", 0), message.get("base_fee", 0),
                                                         height, message.get("msg_method")))
                if message.get("sector_size_value") == 34359738368:
                    prove_gas_32 += bson_to_decimal(message.get("gascost_total_cost")) + aggregate_gas
                    prove_gas_32_count += sector_count
                else:
                    prove_gas_64 += bson_to_decimal(message.get("gascost_total_cost")) + aggregate_gas
                    prove_gas_64_count += sector_count
        if not is_flag:
            return
        obj_dict = dict(
            height_time=local2utc(height_to_datetime(height)),
            pre_gas_32=Decimal128(pre_gas_32),
            pre_gas_count_32=pre_gas_32_count,
            prove_gas_32=Decimal128(prove_gas_32),
            prove_gas_count_32=prove_gas_32_count,
            win_post_gas_32=Decimal128(winpost_gas_32),
            win_post_gas_count_32=winpost_gas_32_count,
            pre_gas_64=Decimal128(pre_gas_64),
            pre_gas_count_64=pre_gas_64_count,
            prove_gas_64=Decimal128(prove_gas_64),
            prove_gas_count_64=prove_gas_64_count,
            win_post_gas_64=Decimal128(winpost_gas_64),
            win_post_gas_count_64=winpost_gas_64_count,
            base_fee=base_fee,
            create_gas_32=Decimal128("0"),
            create_gas_64=Decimal128("0"),
            keep_gas_32=Decimal128("0"),
            keep_gas_64=Decimal128("0"),

        )

        if pre_gas_32_count and prove_gas_32_count:
            obj_dict['create_gas_32'] = Decimal128(
                (pre_gas_32 / pre_gas_32_count + prove_gas_32 / prove_gas_32_count) * _d(32))
        if pre_gas_64_count and prove_gas_64_count:
            obj_dict['create_gas_64'] = Decimal128(
                (pre_gas_64 / pre_gas_64_count + prove_gas_64 / prove_gas_64_count) * _d(16))
        if winpost_gas_32 and winpost_gas_32_count:
            obj_dict['keep_gas_32'] = Decimal128((winpost_gas_32 / winpost_gas_32_count) * _d(32))
        if winpost_gas_64 and winpost_gas_64_count:
            obj_dict['keep_gas_64'] = Decimal128((winpost_gas_64 / winpost_gas_64_count) * _d(16))
        TipsetGas.objects(height=height).upsert_one(**obj_dict)

    @classmethod
    def sync_tipset_gas_stat(cls, height):
        """同步24汽油费相关"""
        height_datetime = height_to_datetime(height)
        table_name = height_datetime.strftime("%Y%m")
        messages = get_db("base")["messages_all@zone_" + table_name].find({"height": height})
        # with switch_collection(MessagesAll, "messages_all@zone_" + height_datetime.strftime("%Y%m")) as Message_all:
        #     messages = Message_all.objects(height=height).no_dereference().exclude().all()
        if messages:
            cls._sync_tipset_gas_stat(height=height, messages=messages)

    @classmethod
    def _sync_tipset_gas_stat(cls, height, messages=[]):
        """24小时内每个tipset的各种gas费汇总"""

        # 删除3天以前的数据
        yesterday = datetime.datetime.now() - datetime.timedelta(days=3)
        TipsetGasStat.objects(height_time__lt=yesterday).delete()

        stat_data = {}
        # 整理数据
        for message in messages:
            sector_type = 0 if message.get("sector_size_value") == 34359738368 else 1
            method = message.get("msg_method_name")
            if not method:
                continue

            key = '%s_%s' % (method, sector_type)

            # 排除费用为0的记录
            total_cost = bson_to_decimal(message.get("gascost_total_cost"))
            if total_cost <= 0:
                continue

            if key not in stat_data:
                stat_data[key] = {
                    'method': method, 'sector_type': sector_type, 'count': 0, 'gas_limit': _d(0),
                    'gas_fee_cap': _d(0), 'gas_premium': _d(0), 'gas_used': _d(0), 'base_fee_burn': _d(0),
                    'over_estimation_burn': _d(0), 'total_cost': _d(0), 'msg_value': _d(0)
                }

            stat_data[key]['count'] += 1
            stat_data[key]['gas_limit'] += _d(message.get("msg_gas_limit", 0))
            stat_data[key]['gas_fee_cap'] += _d(message.get("msg_gas_fee_cap", 0))
            stat_data[key]['gas_premium'] += _d(message.get("msg_gas_premium", 0))
            stat_data[key]['gas_used'] += bson_to_decimal(message.get("gascost_gas_used"))
            stat_data[key]['base_fee_burn'] += bson_to_decimal(message.get("gascost_base_fee_burn"))
            stat_data[key]['over_estimation_burn'] += bson_to_decimal(message.get("gascost_over_estimation_burn"))
            stat_data[key]['total_cost'] += total_cost
            stat_data[key]['msg_value'] += bson_to_decimal(message.get("msg_value"))

        # 新增数据
        for key in stat_data:
            per = stat_data[key]
            obj_dict = dict(
                height_time=height_to_datetime(height),
                count=per['count'],
                gas_limit=Decimal128(per['gas_limit']),
                gas_fee_cap=Decimal128(per['gas_fee_cap']),
                gas_premium=Decimal128(per['gas_premium']),
                gas_used=Decimal128(per['gas_used']),
                base_fee_burn=Decimal128(per['base_fee_burn']),
                over_estimation_burn=Decimal128(per['over_estimation_burn']),
                total_cost=Decimal128(per['total_cost']),
                msg_value=Decimal128(per['msg_value']),
            )
            TipsetGasStat.objects(height=height, method=per['method'], sector_type=per['sector_type']).upsert_one(
                **obj_dict)

    @classmethod
    def get_gas_cost_by_range_height(cls, start_height=None, end_height=None):
        """
        获取生产gas、维护gas
        :param start_height:
        :param end_height:
        :return:
        """
        pipeline = [
            {"$group": {"_id": 0,
                        "sum_pre_gas_32": {"$sum": "$pre_gas_32"},
                        "sum_pre_gas_count_32": {"$sum": "$pre_gas_count_32"},
                        "sum_prove_gas_32": {"$sum": "$prove_gas_32"},
                        "sum_prove_gas_count_32": {"$sum": "$prove_gas_count_32"},
                        "sum_win_post_gas_32": {"$sum": "$win_post_gas_32"},
                        "sum_win_post_gas_count_32": {"$sum": "$win_post_gas_count_32"},
                        "sum_pre_gas_64": {"$sum": "$pre_gas_64"},
                        "sum_pre_gas_count_64": {"$sum": "$pre_gas_count_64"},
                        "sum_prove_gas_64": {"$sum": "$prove_gas_64"},
                        "sum_prove_gas_count_64": {"$sum": "$prove_gas_count_64"},
                        "sum_win_post_gas_64": {"$sum": "$win_post_gas_64"},
                        "sum_win_post_gas_count_64": {"$sum": "$win_post_gas_count_64"},
                        }}
        ]

        data = TipsetGas.objects(height__gt=start_height, height__lte=end_height).aggregate(pipeline)
        tipset_gas = list(data)
        if tipset_gas:
            tipset_gas = tipset_gas[0]
            # 生产成本
            pre_gas_32 = bson_to_decimal(tipset_gas.get("sum_pre_gas_32")) / (
                    tipset_gas.get("sum_pre_gas_count_32") or 1)
            prove_gas_32 = bson_to_decimal(tipset_gas.get("sum_prove_gas_32")) / (
                    tipset_gas.get("sum_prove_gas_count_32") or 1)
            pre_gas_64 = bson_to_decimal(tipset_gas.get("sum_pre_gas_32")) / (
                    tipset_gas.get("sum_pre_gas_count_32") or 1)
            prove_gas_64 = bson_to_decimal(tipset_gas.get("sum_prove_gas_32")) / (
                    tipset_gas.get("sum_prove_gas_count_32") or 1)
            create_gas_32 = (pre_gas_32 + prove_gas_32) * 32
            create_gas_64 = (pre_gas_64 + prove_gas_64) * 16
            # 维护成本
            win_gas_32 = bson_to_decimal(tipset_gas.get("sum_win_post_gas_32")) / (
                    tipset_gas.get("sum_win_post_gas_count_32") or 1)
            win_gas_64 = bson_to_decimal(tipset_gas.get("sum_win_post_gas_64")) / (
                    tipset_gas.get("sum_win_post_gas_count_64") or 1)
            keep_gas_32 = win_gas_32 * 32
            keep_gas_64 = win_gas_64 * 16

            return {'create_gas_32': Decimal128(_d(create_gas_32)), 'create_gas_64': Decimal128(_d(create_gas_64)),
                    'keep_gas_32': Decimal128(_d(keep_gas_32)), 'keep_gas_64': Decimal128(_d(keep_gas_64))}
        return {'create_gas_32': Decimal128("0"), 'create_gas_64': Decimal128("0"),
                'keep_gas_32': Decimal128("0"), 'keep_gas_64': Decimal128("0")}

    @classmethod
    def get_overview_increase_power(cls, height=None, start_height=None, end_height=None):
        """
        获取全网算力增速度
        :param height:
        :param start_height:
        :param end_height
        :return:
        """
        query_dict = {"msg_method_name": {"$in": ["ProveCommitSector", "ProveCommitAggregate"]}}
        if height:
            table_name = height_to_datetime(height).strftime("%Y%m")
            query_dict["height"] = height
        if start_height and end_height:
            table_name = height_to_datetime(end_height).strftime("%Y%m")
            query_dict["height"] = {"$gt": start_height, "$lte": end_height}
        pipeline = [
            {"$match": query_dict},
            {"$group": {
                "_id": 0,
                "increase_power": {"$sum": {"$multiply": ["$sector_count", "$sector_size_value"]}}
            }}]
        result = tuple(get_db("base")["messages@zone_" + table_name].aggregate(pipeline))
        # with switch_collection(Messages, "messages@zone_" + table_name) as Message_:
        #     result = tuple(Message_.objects(**query_dict).aggregate(pipeline))
        if result:
            return _d(result[0]["increase_power"])
        return _d(0)

    @classmethod
    def sync_messages_stat(cls):
        """
        消息统计预先处理
        :return:
        """
        height = 1
        messages_log = MessagesStatLog.objects().first()
        if messages_log:
            height = messages_log.height
            end_height = height + 2880
        else:
            end_height = 2160
        now_height = datetime_to_height(datetime.datetime.now()) - 60
        if end_height > now_height:
            return
        table_name = height_to_datetime(height).strftime("%Y%m")
        pipeline = [
            {"$match": {
                "height": {"$gte": height, "$lt": end_height}
            }},
            {"$group": {
                "_id": {
                    "msg_method_name": "$msg_method_name",
                    "msgrct_exit_code": "$msgrct_exit_code"
                },
                "count": {"$sum": 1}
            }}
        ]
        messages_stats = []
        # with switch_collection(Messages, "messages@zone_" + table_name) as Message_:
        #     data = Message_.objects(height__gte=height, height__lt=end_height).aggregate(pipeline)
        data = get_db("base")["messages@zone_" + table_name].aggregate(pipeline)
        for d in data:
            messages_stats.append(
                UpdateOne({"table_name": table_name,
                           "msg_method_name": d["_id"]["msg_method_name"],
                           "msgrct_exit_code": d["_id"]["msgrct_exit_code"]
                           },
                          {"$inc": {"count": d["count"]}}, upsert=True)
            )
        if messages_stats:
            get_db("business").messages_stat.bulk_write(messages_stats)
        MessagesStatLog.objects().delete()
        MessagesStatLog(height=end_height).save()
        return end_height

    @classmethod
    def sync_messages_wallet(cls):
        """
        处理消息中的钱包转账信息
        :return:
        """
        height = 1
        wallet_flow_log = WalletFlowLog.objects().first()
        if wallet_flow_log:
            height = wallet_flow_log.height
        end_height = height + 1200
        now_height = datetime_to_height(datetime.datetime.now()) - 60  # 避免分叉带来的问题
        if end_height > now_height:
            return
        table_name = height_to_datetime(height).strftime("%Y%m")
        messages_flow = []
        data = get_db("base")["messages@zone_" + table_name].find({"msg_method_name": "Send",
                                                                   "height": {"$gte": height, "$lt": end_height}}).sort(
            [("height", 1)])
        for d in data:
            messages_flow.append(
                UpdateOne({"from_address": d["msg_from"],
                           "to_address": d["msg_to"]},
                          {"$inc": {"count": 1, "value": d["msg_value"]},
                           "$set": {"last_height": d["height"], "last_height_time": d["height_time"]},
                           # "$addToSet": {"msg_id_list": d["msg_cid"]}
                           },
                          upsert=True)
            )
        if messages_flow:
            get_db("business").wallet_flow.bulk_write(messages_flow)
        WalletFlowLog.objects().delete()
        WalletFlowLog(height=end_height).save()
        return end_height


class TipsetService(object):
    """
    高度块服务
    """

    @classmethod
    def get_first_create_gas(cls):
        """
        获取最新高度的生产gas
        :return:
        """
        tipset_gas = TipsetGas.objects().order_by("-height").first()
        return tipset_gas.create_gas_32, tipset_gas.create_gas_64

    @classmethod
    def get_gas_trends(cls, start_date, end_date, step):
        """
        获取gas趋势图
        :return:
        """
        start_height = datetime_to_height(start_date)
        end_height = datetime_to_height(end_date)
        tipset_gas_list = TipsetGas.objects(height__gte=start_height, height__lte=end_height,
                                            height__mod=(step, 0)).all()
        result = [info.to_dict(only_fields=["height", "height_time", "base_fee", "create_gas_32", "create_gas_64"])
                  for info in tipset_gas_list]
        return result

    @classmethod
    def get_gas_stat_all(cls):
        """
        获取完整的24gas统计信息
        :return:
        """

        tipset_stat = TipsetGasStat.objects().order_by("-height").first()
        height = tipset_stat.height

        aggr = {
            "count": {"$sum": 1},
            "gas_limit": {"$sum": "$gas_limit"},
            "gas_fee_cap": {"$sum": "$gas_fee_cap"},
            "gas_premium": {"$sum": "$gas_premium"},
            "gas_used": {"$sum": "$gas_used"},
            "base_fee_burn": {"$sum": "$base_fee_burn"},
            "over_estimation_burn": {"$sum": "$over_estimation_burn"},
            "total_cost": {"$sum": "$total_cost"},
            "msg_value": {"$sum": "$msg_value"},
            "avg_gas_limit": {"$sum": "$gas_limit"},
            "avg_gas_fee_cap": {"$avg": "$gas_fee_cap"},
            "avg_gas_premium": {"$avg": "$gas_premium"},
            "avg_gas_used": {"$avg": "$gas_used"},
            "avg_base_fee_burn": {"$avg": "$base_fee_burn"},
            "avg_over_estimation_burn": {"$avg": "$over_estimation_burn"},
            "avg_cost": {"$avg": "$total_cost"},
        }
        result = {'total': {
            'count': 0,
            'gas_limit': _d(0), 'avg_gas_limit': _d(0),
            'gas_fee_cap': _d(0), 'avg_gas_fee_cap': _d(0),
            'gas_premium': _d(0), 'avg_gas_premium': _d(0),
            'gas_used': _d(0), 'avg_gas_used': _d(0),
            'base_fee_burn': _d(0), 'avg_base_fee_burn': _d(0),
            'over_estimation_burn': _d(0), 'avg_over_estimation_burn': _d(0),
            'total_cost': _d(0), 'avg_cost': _d(0),
            'msg_value': _d(0)
        }}

        def _query(group_tem):
            group_tem.update(aggr)
            return TipsetGasStat.objects(height__gt=height - 2880, height__lte=height).aggregate(
                [{"$group": group_tem}])

        # total
        group_tem = {"_id": 0}
        totals = _query(group_tem)
        totals = list(totals)
        if totals:
            result['total'] = bson_dict_to_decimal(totals[0])

        # group
        group_tem = {"_id": "$method"}
        data = _query(group_tem)
        result.update({d["_id"]: bson_dict_to_decimal(d) for d in data})
        return result

    @classmethod
    def get_overview_create_gas(cls, start_date, end_date, gas_field):
        """
        获取全网生产gas
        :return:
        """
        start_height = datetime_to_height(start_date)
        end_height = datetime_to_height(end_date)
        return TipsetGas.objects(height__gte=start_height, height__lte=end_height).average(gas_field)


class MessageService(object):
    """
    消息服务
    """

    @classmethod
    def get_mpool_list(cls, key_words=None, method_name=None, page_index=1, page_size=20):
        """
        获取消息池列表
        :param key_words:
        :param method_name:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if key_words:
            query = (Q(msg_from=key_words) | Q(msg_to=key_words))
        method_name_list = Mpool.objects(query).distinct("method_name")
        if method_name:
            query &= Q(method_name=method_name)
        mpools = Mpool.objects(query).order_by("-height")
        result = mongo_paginator(mpools, page_index, page_size)
        data = []
        for info in result['objects']:
            tmp = info.to_dict(only_fields=("msg_from", "msg_to", "cid", "method_name", "height",
                                            "value", "gas_limit", "gas_premium"))
            tmp["height_time"] = height_to_datetime(tmp["height"], need_format=True)
            data.append(tmp)
        result['objects'] = data
        result["method_name_list"] = method_name_list
        return result

    @classmethod
    def get_mpool_info(cls, cid):
        """
        获取消息池消息
        :param cid:
        :return:
        """

        mpool = Mpool.objects(cid=cid).first()
        if mpool:
            tmp = mpool.to_dict()
            tmp["height_time"] = height_to_datetime(tmp["height"], need_format=True)
            return tmp
        return {}

    @classmethod
    def get_message_list(cls, miner_id=None, block_hash=None, msg_method=None, msgrct_exit_code=None, height=None,
                         start_date=None, end_date=None, is_transfer=False, page_index=1, page_size=20):
        """
        获取消息列表
        :param miner_id:
        :param block_hash:
        :param msg_method:
        :param msgrct_exit_code:
        :param height:
        :param start_date:
        :param end_date:
        :param is_transfer:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if miner_id:
            query = (Q(msg_to=miner_id) | Q(msg_from=miner_id))
        if height:
            query &= Q(height=height)
        if block_hash:
            messages_list = []
            height = 1
            block_messages = BlockMessages.objects(block=block_hash).first()
            if block_messages:
                messages_list = block_messages.messages
                height = block_messages.height
            query = Q(msg_cid__in=messages_list)
        if msg_method:
            query &= Q(msg_method_name=msg_method)
        if msgrct_exit_code:
            query &= Q(msgrct_exit_code=msgrct_exit_code)
        if start_date:
            start_height = datetime_to_height(datetime.datetime.strptime(start_date + ' 00:00:00', "%Y-%m-%d %H:%M:%S"))
            query &= Q(height__gte=start_height)
        if end_date:
            end_height = datetime_to_height(datetime.datetime.strptime(end_date + ' 23:59:59', "%Y-%m-%d %H:%M:%S"))
            query &= Q(height__lte=end_height)
        if is_transfer and not msg_method:
            query &= Q(msg_method_name__in=["Send", "WithdrawBalance"])
        query_dict = query.to_query(Messages)
        if is_transfer:
            table_dict, total_count = cls.get_message_count(query_dict)
        else:
            # 带条件消息count获取
            if miner_id:
                table_dict, total_count = cls.get_message_count_by_msg_key(miner_id or block_hash, msg_method,
                                                                           msgrct_exit_code, query_dict)
            elif block_hash or height:
                table_dict, total_count = cls.get_message_count(query_dict, height)
            else:
                table_dict, total_count = cls.get_message_count_all(msg_method, msgrct_exit_code)
        print(table_dict)
        result = cls.get_page_offset_limit(table_dict, total_count, page_index=page_index, page_size=page_size)
        data = []
        new_table_dict = result.pop("new_table_dict", {})
        print(new_table_dict)
        print(query)
        for table_name, offset_limit in new_table_dict.items():
            tmps = get_db("base")["messages@zone_" + table_name].find(query_dict).sort([("height", -1)]).skip(
                offset_limit[0]).limit(offset_limit[1])
            for info in tmps:
                tmp = {
                    "msg_from": info["msg_from"],
                    "msg_to": info["msg_to"],
                    "msg_cid": info["msg_cid"],
                    "msg_method_name": info["msg_method_name"],
                    "height": info["height"],
                    "msg_value": bson_to_decimal(info["msg_value"]),
                    "msgrct_exit_code": info["msgrct_exit_code"],
                    "height_time": utc2local(info["height_time"]),
                }
                data.append(tmp)
        result["objects"] = data
        # print(datetime.datetime.now() - now)
        return result

    @classmethod
    def get_message_count_by_msg_key(cls, msg_value, msg_method, msgrct_exit_code, query=None):
        """
        更加查询条件统计消息数量
        :param query:
        :return:
        """
        # 查询是否已经有key
        msg_key = "{0}|{1}|{2}".format(msg_value, msg_method, msgrct_exit_code)
        table_dict = OrderedDict()  # 表字典
        total_count = 0
        data = MessagesStatInfo.objects(msg_key=msg_key).order_by("-table_name").all()
        for d in data:
            table_dict[d.table_name] = d.count
            total_count += d.count
        now_str = datetime.datetime.now().strftime("%Y%m")
        if table_dict and table_dict.get(now_str):
            count = get_db("base")["messages@zone_" + now_str].count(query)
            table_dict[now_str] = count
            total_count = sum(table_dict.values())
            table_dict.move_to_end(now_str, last=False)
        else:
            table_dict, total_count = cls.get_message_count(query=query)
        cls.save_messages_stat_info(table_dict, msg_key)
        return table_dict, total_count

    @classmethod
    def get_message_count_all(cls, msg_method, msgrct_exit_code):
        """
        消息全数据查询
        :param msg_method:
        :param msgrct_exit_code:
        :return:
        """
        # 查询是否已经有key
        table_dict = OrderedDict()  # 表字典
        total_count = 0
        query_dict = {}
        if msg_method:
            query_dict["msg_method_name"] = msg_method
        if msgrct_exit_code:
            query_dict["msgrct_exit_code"] = msgrct_exit_code
        pipeline = [
            {"$group": {
                "_id": "$table_name",
                "total_count": {"$sum": "$count"}
            }},
            {"$sort": {"_id": -1}}
        ]
        data = MessagesStat.objects(**query_dict).aggregate(pipeline)
        for d in data:
            table_dict[d["_id"]] = d["total_count"]
            total_count += d["total_count"]
        if table_dict:
            now_str = datetime.datetime.now().strftime("%Y%m")
            height = datetime_to_height(datetime.datetime.now().date())
            query_dict["height"] = {"$gte": height}
            count = get_db("base")["messages@zone_" + now_str].count(query_dict)
            # with switch_collection(Messages, "messages@zone_" + now_str) as Message_s:
            #     count = Message_s._get_collection().count(query_dict)
            total_count += count
            table_dict[now_str] = count + table_dict.get(now_str, 0)
            table_dict.move_to_end(now_str, last=False)
        else:
            table_dict, total_count = cls.get_message_count(query_dict)
        return table_dict, total_count

    @classmethod
    def get_message_count(cls, query=None, height=None):
        """
        更加查询条件统计消息数量
        :param query:
        :param height:
        :return:
        """
        # 生成表名称
        if height:
            tables = [height_to_datetime(height).strftime("%Y%m")]
        else:
            now_str = datetime.datetime.now().strftime("%Y-%m-%d")
            tables = pd.date_range('2020-8-1', now_str, freq='MS').strftime("%Y%m").tolist()
            tables.reverse()
        table_dict = OrderedDict()  # 表字典
        total_count = 0
        for table_name in tables:
            count = get_db("base")["messages@zone_" + table_name].count(query)
            total_count += count
            table_dict[table_name] = count
        return table_dict, total_count

    @classmethod
    def get_message_method_list(cls, miner_id=None, block_hash=None, start_date=None, end_date=None, is_transfer=False):
        """
        更加查询条件统计消息方法类型
        :param miner_id:
        :param block_hash:
        :param start_date:
        :param end_date:
        :param is_transfer:
        :return:
        """
        # 生成表名称
        query = Q()
        if miner_id:
            query = (Q(msg_to=miner_id) | Q(msg_from=miner_id))
        if block_hash:
            messages_list = []
            height = 1
            block_messages = BlockMessages.objects(block=block_hash).first()
            if block_messages:
                messages_list = block_messages.messages
                height = block_messages.height
            query = Q(msg_cid__in=messages_list)
        if start_date:
            start_height = datetime_to_height(datetime.datetime.strptime(start_date + ' 00:00:00', "%Y-%m-%d %H:%M:%S"))
            query &= Q(height__gte=start_height)
        if end_date:
            end_height = datetime_to_height(datetime.datetime.strptime(end_date + ' 23:59:59', "%Y-%m-%d %H:%M:%S"))
            query &= Q(height__lte=end_height)
        if is_transfer:
            query &= Q(msg_method_name__in=["Send", "WithdrawBalance"])
        query_dict = query.to_query(Messages)
        if block_hash and height:
            tables = [height_to_datetime(height).strftime("%Y%m")]
        else:
            now_str = datetime.datetime.now().strftime("%Y-%m-%d")
            tables = pd.date_range('2020-8-1', now_str, freq='MS').strftime("%Y%m").tolist()
            tables.reverse()
        method_set = set()
        for table_name in tables:
            msg_method_names = get_db("base")["messages@zone_" + table_name].distinct("msg_method_name", query_dict)
            # with switch_collection(Messages, "messages@zone_" + table_name) as Message_s:
            #     msg_method_names = Message_s.objects(query).distinct("msg_method_name")
            method_set.update(msg_method_names)
        if "" in method_set:
            method_set.remove("")
        return sorted(method_set)

    @classmethod
    def save_messages_stat_info(cls, table_dict, msg_key):
        """
        保持查询条件的数据,使下一次查询速度加快
        :param table_dict:
        :param msg_key:
        :return:
        """

        messages_stats = []
        for table_name, count in table_dict.items():
            messages_stats.append(
                UpdateOne({"msg_key": msg_key,
                           "table_name": table_name
                           },
                          {"$set": dict(count=count)}, upsert=True))
        if messages_stats:
            get_db("business").messages_stat_info.bulk_write(messages_stats)

    @classmethod
    def get_page_offset_limit(cls, table_dict, table_count, page_index=1, page_size=20):
        """
         分页的定位计算
        :return:
        """
        new_table_dict = OrderedDict()  # 点位后的字典表
        offset = page_size * (page_index - 1)
        pos = 0  # 循环表的总记录数
        count = 0  # 返回数据的累计条数
        for k, v in table_dict.items():
            if v == 0:
                continue
            pos += v
            if pos <= offset:
                continue
            if new_table_dict:
                page_offset = 0
                diff = page_size - count  # 还缺少的数据数量
                page_limit = v if diff > v else diff
            else:  # 第一次查询位置
                page_offset = v - (pos - offset)
                page_limit = page_size if pos - offset > page_size else pos - offset
            new_table_dict[k] = [page_offset, page_limit]
            count += page_limit
            if count >= page_size:
                break
        return {
            "new_table_dict": new_table_dict,
            'page_index': page_index,
            'page_size': page_size,
            'total_page': math.ceil(table_count / page_size),
            'total_count': table_count
        }

    @classmethod
    def get_message_info(cls, msg_cid, height):
        """
        获取消息
        :param msg_cid:
        :param height:
        :return:
        """
        if not height:
            message = cls.get_is_message(msg_cid)
            if message:
                height = message["height"]
        height = int(height)
        table_name = height_to_datetime(height).strftime("%Y%m")
        tip_miner_id = ""
        block_message = BlockMessages.objects(height=height, messages=msg_cid).first()
        if block_message:
            block = Blocks.objects(block_hash=block_message.block).only("miner_id").first()
            tip_miner_id = block and block.miner_id

        message = get_db("base")["messages_all@zone_" + table_name].find_one({"msg_cid": msg_cid})
        # with switch_collection(MessagesAll, "messages_all@zone_" + table_name) as Message_s:
        #     message = Message_s.objects(msg_cid=msg_cid).first()
        if message:
            data = bson_dict_to_decimal(message)
            data["height_time"] = utc2local(data["height_time"])
            data.pop("_id", None)
            data["tip_miner_id"] = tip_miner_id
            # 消耗费用
            data["fee_burn"] = data.get('gascost_base_fee_burn', 0) + data.get('gascost_over_estimation_burn', 0)
            # batch_gas_charge
            if message.get("msgrct_exit_code") == 0:
                message_none = get_db("base")["messages@zone_" + table_name].find_one({"msg_cid": msg_cid})
                data['batch_gas_charge'] = _d(
                    get_aggregate_gas(message_none.get("sector_count", 0), int(data["base_fee"]),
                                      data["height"], data["msg_method"]))
            return data
        return {}

    @classmethod
    def get_is_message(cls, value):
        """
        判断是否是block
        :param value:
        :return:
        """
        # 生成表名称
        now_str = datetime.datetime.now().strftime("%Y-%m-%d")
        tables = pd.date_range('2020-8-1', now_str, freq='MS').strftime("%Y%m").tolist()
        tables.reverse()
        result = None
        for table_name in tables:
            result = get_db("base")["messages@zone_" + table_name].find_one({"msg_cid": value})
            if result:
                break
        return result

    @classmethod
    def get_message_list_by_height(cls, height):
        """
        根据高度获取转账消息
        :param height:
        :return:
        """
        if not height:
            table_name = datetime.datetime.now().strftime("%Y%m")
            result = get_db("base")["messages@zone_" + table_name].find_one(sort=[("height", -1)])
            height = result.get("height")
        else:
            table_name = height_to_datetime(height).strftime("%Y%m")
            if not get_db("base")["messages@zone_" + table_name].find_one({"height": int(height)}):
                if Tipset.objects(height=int(height)).first():
                    return {"height": height, "data": []}
                return {}
        query_dict = {"height": int(height), "msg_method_name": "Send", "msgrct_exit_code": 0}
        tmps = get_db("base")["messages@zone_" + table_name].find(query_dict)
        data = []
        for info in tmps:
            tmp = {
                "msg_from": info["msg_from"],
                "msg_to": info["msg_to"],
                "msg_cid": info["msg_cid"],
                # "msg_method_name": info["msg_method_name"],
                # "height": info["height"],
                "msg_value": bson_to_decimal(info["msg_value"]),
                # "height_time": utc2local(info["height_time"]),
            }
            data.append(tmp)
        return {"height": height, "data": data}

    @classmethod
    def get_gas_cost_by_miner_no(cls, miner_no):
        """
        根据矿工ID获取24小时矿工信息
        :param miner_no:
        :return:
        """
        # 单个矿工的生产gas 这里这能预估大概不是准确的
        # pre+prove+过期质押/算力增加量
        now_time = datetime.datetime.now()
        start_time = now_time - datetime.timedelta(days=1)
        start_height = datetime_to_height(start_time)
        end_height = datetime_to_height(now_time)
        table_names = set()
        table_names.add(start_time.strftime("%Y%m"))
        table_names.add(now_time.strftime("%Y%m"))
        method_list = ["PreCommitSector", "ProveCommitSector", "SubmitWindowedPoSt", "PreCommitSectorBatch",
                       "ProveCommitAggregate"]
        pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$in": method_list}}},
            {"$group": {
                "_id": "$msg_method_name",
                "gascost_total_cost": {"$sum": "$gascost_total_cost"},
                "msg_value": {"$sum": "$msg_value"}
            }}]
        create_gas = _d(0)
        winpost_gas = _d(0)
        pledge_gas = _d(0)
        for table_name in table_names:
            data = get_db("base")["messages@zone_" + table_name].aggregate(pipeline)

            for per in data:
                if per['_id'] == "SubmitWindowedPoSt":
                    winpost_gas += bson_to_decimal(per['gascost_total_cost'])
                else:
                    create_gas += bson_to_decimal(per['gascost_total_cost'])
                    pledge_gas += bson_to_decimal(per['msg_value'])
        return create_gas, winpost_gas, pledge_gas, create_gas + winpost_gas + pledge_gas

    @classmethod
    def get_gas_cost_stat_by_miner_no(cls, miner_no, stat_type="24h"):
        """
        根据矿工ID获取指定时间段矿工gas消耗统计分析
        :param miner_no:
        :param stat_type:
        :return:
        """
        height = 2880
        if stat_type == "7d":
            height *= 7
        if stat_type == "30d":
            height *= 30
        end_height = datetime_to_height(datetime.datetime.now())
        start_height = end_height - height
        table_names = set()
        table_names.add(height_to_datetime(start_height).strftime("%Y%m"))
        table_names.add(height_to_datetime(end_height).strftime("%Y%m"))
        method_list = ["PreCommitSector", "ProveCommitSector", "SubmitWindowedPoSt", "PreCommitSectorBatch",
                       "ProveCommitAggregate"]
        pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$in": method_list}}},
            {"$group": {
                "_id": "$msg_method_name",
                "gascost_total_cost": {"$sum": "$gascost_total_cost"}
            }}]
        pipeline_total = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height}}},
            {"$group": {
                "_id": 0,
                "gascost_total_cost": {"$sum": "$gascost_total_cost"}
            }}]

        result = {"total_gas": _d(0), "SubmitWindowedPoSt": _d(0), "PreCommitSector": _d(0),
                  "ProveCommitSector": _d(0), "PreCommitSectorBatch": _d(0),
                  "ProveCommitAggregate": _d(0), "other_gas": _d(0)}
        for table_name in table_names:
            data = get_db("base")["messages@zone_" + table_name].aggregate(pipeline)
            for per in data:
                result.setdefault(per["_id"], _d(0))
                result[per["_id"]] += bson_to_decimal(per["gascost_total_cost"])
            result.setdefault("total_gas", _d(0))
            data_total = get_db("base")["messages@zone_" + table_name].aggregate(pipeline_total)
            result["total_gas"] += bson_to_decimal(list(data_total)[0].get("gascost_total_cost"))

        result["other_gas"] = result["total_gas"] - result["SubmitWindowedPoSt"] - result["PreCommitSector"] \
                              - result["ProveCommitSector"] - result["PreCommitSectorBatch"] - result[
                                  "ProveCommitAggregate"]
        return result

    @classmethod
    def get_messages_stat_by_miner_no(cls, miner_no, stat_type="24h"):
        """
        根据矿工ID获取指定时间段矿工gas消耗统计分析
        :param miner_no:
        :param stat_type:
        :return:
        """
        height = 2880
        if stat_type == "7d":
            height *= 7
        if stat_type == "30d":
            height *= 30
        end_height = datetime_to_height(datetime.datetime.now())
        start_height = end_height - height
        table_names = set()
        table_names.add(height_to_datetime(start_height).strftime("%Y%m"))
        table_names.add(height_to_datetime(end_height).strftime("%Y%m"))
        method_list = ["PreCommitSector", "ProveCommitSector", "SubmitWindowedPoSt", "PreCommitSectorBatch",
                       "ProveCommitAggregate"]
        pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$in": method_list}, "msgrct_exit_code": 0}},
            {"$group": {
                "_id": "$msg_method_name",
                "count": {"$sum": 1}
            }}]

        not_pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$in": method_list}, "msgrct_exit_code": {"$ne": 0}}},
            {"$group": {
                "_id": "$msg_method_name",
                "count": {"$sum": 1}
            }}]

        others_pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$nin": method_list}, "msgrct_exit_code": 0}},
            {"$group": {
                "_id": 0,
                "count": {"$sum": 1}
            }}]
        others_not_pipeline = [
            {"$match": {"msg_to": miner_no, "height": {"$gte": start_height, "$lte": end_height},
                        "msg_method_name": {"$nin": method_list}, "msgrct_exit_code": {"$ne": 0}}},
            {"$group": {
                "_id": 0,
                "count": {"$sum": 1}
            }}]

        result = {'SubmitWindowedPoSt': {"count": 0, "ok_count": 0, "error_count": 0},
                  'PreCommitSector': {"count": 0, "ok_count": 0, "error_count": 0},
                  'ProveCommitSector': {"count": 0, "ok_count": 0, "error_count": 0},
                  'PreCommitSectorBatch': {"count": 0, "ok_count": 0, "error_count": 0},
                  'ProveCommitAggregate': {"count": 0, "ok_count": 0, "error_count": 0},
                  'others': {"count": 0, "ok_count": 0, "error_count": 0},
                  }
        for table_name in table_names:
            for per in get_db("base")["messages@zone_" + table_name].aggregate(pipeline):
                result[per["_id"]]["ok_count"] += per["count"]
            for per1 in get_db("base")["messages@zone_" + table_name].aggregate(not_pipeline):
                result[per1["_id"]]["error_count"] += per1["count"]
            for per2 in get_db("base")["messages@zone_" + table_name].aggregate(others_pipeline):
                result["others"]["ok_count"] += per2["count"]
            for per3 in get_db("base")["messages@zone_" + table_name].aggregate(others_not_pipeline):
                result["others"]["error_count"] += per3["count"]

        for method_name, count_dict in result.items():
            count_dict['count'] = count_dict['ok_count'] + count_dict['error_count']
            result[method_name]["ok_rate"] = round(count_dict["ok_count"] / count_dict['count'], 4) if count_dict[
                'count'] else 0

        return result
