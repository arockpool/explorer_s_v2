import datetime
from mongoengine import Q
from base.utils.fil import _d, height_to_datetime, datetime_to_height, bson_to_decimal
from explorer.models.deal import Deal, Dealinfo,DealDay,DealStat
from explorer.models.miner import MinerDealPriceHistory
from base.utils.paginator import mongo_paginator
from mongoengine.connection import get_db
from bson.decimal128 import Decimal128


class DealService(object):
    """
    订单服务
    """

    @classmethod
    def get_deal_list(cls, key_words=None, is_verified=0, start_time_height=None, end_time_height=None, page_index=1,
                      page_size=20):
        """
        获取订单列表
        :param key_words:
        :param is_verified:
        :param start_time_height:
        :param end_time_height:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if key_words:
            if key_words.isdigit():
                query = Q(deal_id=key_words)
            else:
                query = (Q(client=key_words) | Q(provider=key_words))
        if is_verified == 1:
            query &= Q(is_verified=True)
        if is_verified == 0:
            query &= Q(is_verified=False)
        if start_time_height:
            query &= Q(height__gte=start_time_height)
        if end_time_height:
            query &= Q(height__lte=end_time_height)
        query = Deal.objects(query).order_by("-deal_id")
        result = mongo_paginator(query, page_index, page_size)
        result['objects'] = [info.to_dict(only_fields=("deal_id", "height_time", "client", "provider", "piece_size",
                                                       "is_verified", "storage_price_per_epoch"))
                             for info in result['objects']]

        return result

    @classmethod
    def get_deal_info(cls, deal_id):
        """
        获取订单详情
        :param deal_id:
        :return:
        """

        wallet = Deal.objects(deal_id=deal_id).first()
        data = wallet.to_dict()
        data["start_time"] = height_to_datetime(data["start_epoch"], need_format=True)
        data["end_time"] = height_to_datetime(data["end_epoch"], need_format=True)
        return data

    @classmethod
    def get_deal_info_list(cls, key_words=None, is_verified=0, start_time_height=None, end_time_height=None,
                           page_index=1, page_size=20):
        """
        获取订单列表
        :param key_words:
        :param is_verified:
        :param start_time_height:
        :param end_time_height:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if key_words:
            if key_words.isdigit():
                query = Q(deal_id=key_words)
            else:
                query = (Q(client=key_words) | Q(provider=key_words))
        if is_verified == 1:
            query &= Q(verified_deal=True)
        if is_verified == 0:
            query &= Q(verified_deal=False)
        if start_time_height:
            query &= Q(sector_start_epoch__gte=start_time_height)
        if end_time_height:
            query &= Q(sector_start_epoch__lte=end_time_height)
        query = Dealinfo.objects(query).order_by("-sector_start_epoch")
        result = mongo_paginator(query, page_index, page_size)
        result['objects'] = [info.to_dict() for info in result['objects']]
        return result


class DealStatService(object):

    @classmethod
    def sync_deal_stat(cls):
        """
        24小订单统计
        :return:
        """
        obj_dict = {}
        now_height = datetime_to_height(datetime.datetime.now())
        height = now_height - 2880
        # 所有数据
        deal_data = Deal.objects(height__gte=height).aggregate([
            {"$group": {"_id": 0,
                        "data_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        deal_data = list(deal_data)
        deal_data = deal_data[0] if deal_data else {}
        obj_dict["deal_count"] = deal_data.get("count", 0)
        obj_dict["data_size"] = bson_to_decimal(deal_data.get("data_size", Decimal128("0")))
        # 验证的数据
        verified_deal_data = Deal.objects(height__gte=height, is_verified=True).aggregate([
            {"$group": {"_id": 0,
                        "data_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        verified_deal_data = list(verified_deal_data)
        verified_deal_data = verified_deal_data[0] if verified_deal_data else {}
        obj_dict["verified_deal_count"] = verified_deal_data.get("count", 0)
        obj_dict["verified_data_size"] = bson_to_decimal(verified_deal_data.get("data_size", Decimal128("0")))
        # 活跃客户
        client_data = Deal.objects(height__gte=height).aggregate([
            {"$project": {"client": 1}},
            {"$group": {"_id": "$client"}},
            {"$group": {"_id": 0,
                        "count": {"$sum": 1}}},
        ])
        client_data = list(client_data)
        client_data = client_data[0] if client_data else {}
        obj_dict["client_count"] = client_data.get("count", 0)
        # 文件数
        piece_cid_data = Deal.objects(height__gte=height).aggregate([
            {"$project": {"piece_cid": 1}},
            {"$group": {"_id": "$piece_cid"}},
            {"$group": {"_id": 0,
                        "count": {"$sum": 1}}},
        ])
        piece_cid_data = list(piece_cid_data)
        piece_cid_data = piece_cid_data[0] if piece_cid_data else {}
        obj_dict["piece_cid_count"] = piece_cid_data.get("count", 0)
        # 成本
        total_gas = _d(0)
        pipeline = [
            {"$match": {"height": {"$gte": height}, "msg_method_name": "PublishStorageDeals"}},
            {"$group": {"_id": 0,
                        "gascost_total_cost": {"$sum": "$gascost_total_cost"}}},
        ]
        table_names = set()
        table_names.add(height_to_datetime(height).strftime("%Y%m"))
        table_names.add(height_to_datetime(now_height).strftime("%Y%m"))
        for table_name in table_names:
            for per in get_db("base")["messages@zone_" + table_name].aggregate(pipeline):
                total_gas += bson_to_decimal(per.get("gascost_total_cost"))
        deal_gas_by_t = total_gas / (deal_data.get("data_size") / _d(1024 ** 4))
        obj_dict["deal_gas_by_t"] = deal_gas_by_t
        DealStat.objects(height=height).upsert_one(**obj_dict)
        # 删除7天前的数据
        DealStat.objects(height__lte=height-(7*2880)).delete()

    @classmethod
    def sync_deal_day(cls, date_str=None):
        now_date = datetime.datetime.now().date()
        if date_str:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

        else:
            day_date = "2020-10-15"
            deal_day = DealDay.objects().order_by("-date").first()
            if deal_day:
                day_date = deal_day.date
            date = (datetime.datetime.strptime(day_date, "%Y-%m-%d") + datetime.timedelta(days=1)).date()
            if date > now_date - datetime.timedelta(days=1):
                return
            date_str = date.strftime("%Y-%m-%d")
        # 当天的最后一个高度数据
        obj_dict={}
        height = datetime_to_height(date)
        client_count = len(Deal.objects(height__gte=height, height__lt=height + 2880).distinct(
            "client"))
        obj_dict["client_count"]=client_count
        # 所有数据
        deal_data = Deal.objects(height__gte=height, height__lt=height + 2880).aggregate([
            {"$group": {"_id": 0,
                        "data_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        deal_data = list(deal_data)
        deal_data = deal_data[0] if deal_data else {}
        obj_dict["deal_count"] = deal_data.get("count", 0)
        obj_dict["data_size"] = bson_to_decimal(deal_data.get("data_size", Decimal128("0")))
        # 验证的数据
        verified_deal_data = Deal.objects(height__gte=height, height__lt=height + 2880, is_verified=True).aggregate([
            {"$group": {"_id": 0,
                        "data_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        verified_deal_data = list(verified_deal_data)
        verified_deal_data = verified_deal_data[0] if verified_deal_data else {}
        obj_dict["verified_deal_count"] = verified_deal_data.get("count", 0)
        obj_dict["verified_data_size"] = bson_to_decimal(verified_deal_data.get("data_size", Decimal128("0")))
        # 成本
        total_gas = _d(0)
        pipeline = [
            {"$match": {"height": {"$gte": height, "$lt": height + 2880}, "msg_method_name": "PublishStorageDeals"}},
            {"$group": {"_id": 0,
                        "gascost_total_cost": {"$sum": "$gascost_total_cost"}}},
        ]
        table_names = set()
        table_names.add(height_to_datetime(height).strftime("%Y%m"))
        table_names.add(height_to_datetime(height + 2880).strftime("%Y%m"))
        for table_name in table_names:
            for per in get_db("base")["messages@zone_" + table_name].aggregate(pipeline):
                total_gas += bson_to_decimal(per.get("gascost_total_cost"))
        deal_gas_by_t = total_gas / (deal_data.get("data_size") / _d(1024 ** 4))
        obj_dict["deal_gas_by_t"] = deal_gas_by_t
        return DealDay.objects(date=date_str).upsert_one(**obj_dict).id

    @classmethod
    def get_deal_stat(cls):
        """
        24小时的订单数据
        :return:
        """
        deal_stat = DealStat.objects().order_by("-height").first()
        result = deal_stat.to_dict()
        # result["avg_price"] = bson_to_decimal(MinerDealPriceHistory.objects(price__gt=0).average("price"))
        return result

    @classmethod
    def get_deal_day(cls, stats_type):
        """
        按天获取订单数据
        :param stats_type:
        :return:
        """
        limit = int(stats_type[0:stats_type.find("d")])
        days = DealDay.objects().order_by("-date").limit(limit)
        data = []
        for info in days:
            tmp = info.to_dict()
            data.append(tmp)
        if limit == 90:
            data = data[::3]
        if limit == 180:
            data = data[::6]
        if limit == 360:
            data = data[::120]
        return data
