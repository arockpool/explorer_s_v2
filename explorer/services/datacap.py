import datetime, json
import decimal
import pandas as pd
from mongoengine import Q
from pymongo import ReplaceOne, UpdateOne, IndexModel, ASCENDING, DESCENDING
from base.utils.fil import _d, height_to_datetime, datetime_to_height, local2utc, bson_to_decimal
from explorer.models.datacap import Notaries, PlusClient, Provider, DatacapDay
from explorer.models.wallets import Wallets
from explorer.models.deal import Deal
from mongoengine.connection import get_db
from base.utils.paginator import mongo_paginator
from bson.decimal128 import Decimal128
from base.third import sc01, requs, bbhe_sdk
from base.flask_ext import cache

class DataCapService(object):

    @classmethod
    def sync_notaries(cls):
        """
        同步公证人数据
        :return:
        """
        notaries_data = sc01.initial_notaries_data()
        notaries_list = []
        for notaries in notaries_data:
            id_address = notaries["id_address"]
            if id_address == "--":
                wallets = Wallets.objects(address=notaries["address"]).first()
                if wallets:
                    id_address = wallets.id
            notaries_dict = dict(
                id_address=id_address,
                name=notaries["name"],
                org=notaries["org"],
                address=notaries["address"],
                region=notaries["region"],
                media=notaries["media"],
                use_case=notaries["use_case"],
                application_link=notaries["application_link"],
                github_account=notaries["github_account"],
                granted_allowance=Decimal128(notaries["granted_allowance"]),
                create_time=datetime.datetime.utcnow()
            )
            insert_dict = dict(
                allocated_allowance=Decimal128("0"),
                client_count=0,
                clients=[],
            )
            notaries_list.append(UpdateOne({"address": notaries["address"]},
                                           {"$set": notaries_dict, "$setOnInsert": insert_dict}, upsert=True))
        modified_count = 0
        if notaries_list:
            modified_count = get_db("business").notaries.bulk_write(notaries_list).modified_count
        # 统计数量,补全allowance因为github导致出入
        cls._repair_notaries()
        return modified_count

    @classmethod
    def sync_plus_client(cls):
        """
        同步公证人数据
        :return:
        """
        datacap_data = requs.initial_req()
        plus_client_list = []
        for client in datacap_data:
            id_address = "--"
            wallets = Wallets.objects(address=client["allocated_address"]).first()
            if wallets:
                id_address = wallets.id
            plus_client_dict = dict(
                id_address=id_address,
                name=client["name"],
                address=client["allocated_address"],
                region=client["region"],
                media=client["media"],
                # notaries = fields.DictField(help_text='公证人信息')
                msg_cid=client["msg_cid"],
                comments_url=client["comments_url"],
                assignor=client["assignor"],
                assignee=client["assignee"],
                create_time=datetime.datetime.utcnow()
            )
            # insert_dict = dict(
            #     allocated_allowance=Decimal128(client["allocated_datacap"]),
            #     use_allowance=Decimal128("0"),
            #     deal_count=0,
            #     provider_count=0,
            #     providers=[],
            # )
            plus_client_list.append(
                UpdateOne({"address": client["allocated_address"]},{"$set": plus_client_dict}))
        modified_count = 0
        if plus_client_list:
            modified_count = get_db("business").plus_client.bulk_write(plus_client_list).modified_count
        return modified_count

    @classmethod
    def sync_add_verified_client(cls):
        """
        同步验证客户消息
        :return:
        """
        cur_height = 1
        plus_client_log = get_db("business").plus_client_log.find_one()
        if plus_client_log:
            cur_height = plus_client_log.get("height")
        cur_str = height_to_datetime(cur_height).strftime("%Y-%m")
        now_str = datetime.datetime.now().strftime("%Y-%m-%d")
        tables = pd.date_range(cur_str+"-01", now_str, freq='MS').strftime("%Y%m").tolist()
        notaries_list = []
        plus_client_list = []
        height = cur_height
        for table_name in tables:
            get_db("base")["messages_all@zone_" + table_name].ensure_index(
                [("msg_method_name", ASCENDING), ("height", DESCENDING)], name="msg_method_name_1_height_-1")
            get_db("base")["messages_all@zone_" + table_name].ensure_index([("msg_cid", ASCENDING)], name="msg_cid_1")
            messages = get_db("base")["messages_all@zone_" + table_name].find({"msg_method_name": "AddVerifiedClient",
                                                                               "height": {"$gt": cur_height}}).sort(
                [("height", 1)])
            for message in messages:
                height = message["height"]
                notaries_address = message["msg_from"]
                msg_params = json.loads(message.get("msg_params") or "{}")
                plus_client_address = msg_params.get("Address")
                allocated_allowance = msg_params.get("Allowance")
                # 公证人
                # notaries = Notaries.objects(address=notaries_address).first()
                # if notaries:
                plus_client_dict = dict(
                    notaries={"address": notaries_address}
                )
                notaries_list.append(UpdateOne({"address": notaries_address},
                                               {"$inc": {"allocated_allowance": Decimal128(allocated_allowance)},
                                                "$addToSet": {"clients": plus_client_address}},
                                               upsert=True))
                # 客户
                insert_dict = dict(
                    use_allowance=Decimal128("0"),
                    deal_count=0,
                    provider_count=0,
                    providers=[],
                )
                wallets = Wallets.objects(address=plus_client_address).first()
                if wallets:
                    plus_client_dict["id_address"] = wallets.id
                plus_client = {
                             "$setOnInsert": insert_dict,
                             "$inc": {"allocated_allowance": Decimal128(allocated_allowance)}}
                if plus_client_dict:
                    plus_client["$set"] = plus_client_dict
                plus_client_list.append(UpdateOne({"address": plus_client_address}, plus_client, upsert=True))
        modified_count = 0
        if plus_client_list:
            modified_count = get_db("business").plus_client.bulk_write(plus_client_list).modified_count
        if notaries_list:
            get_db("business").notaries.bulk_write(notaries_list)
        get_db("business").plus_client_log.delete_many({})
        get_db("business").plus_client_log.insert_one({"height": height})
        # 统计数量,补全allowance因为github导致出入
        cls._repair_notaries()
        return modified_count

    @classmethod
    def _repair_notaries(cls):
        """
        统计数量,补全allowance因为github导致出入
        :return:
        """
        # 同步客户中公证人的数据
        plus_client_list = []
        for client in PlusClient.objects().all():
            notaries_address = client.notaries.get("address")
            if notaries_address:
                notaries = Notaries.objects(address=client.notaries["address"]).first()
                if notaries:
                    plus_client_dict = dict(
                        notaries={"address": notaries.address, "id_address": notaries.id_address, "name": notaries.name}
                    )
                    plus_client_list.append(UpdateOne({"address": client.address}, {"$set": plus_client_dict}))
        if plus_client_list:
            get_db("business").plus_client.bulk_write(plus_client_list)
        # 因为数据量小
        for notaries in Notaries.objects().all():
            if notaries.allocated_allowance > notaries.granted_allowance:
                notaries.granted_allowance = notaries.allocated_allowance
            notaries.client_count = PlusClient.objects(notaries__id_address=notaries.id_address, id_address__ne=None).count()
            notaries.save()

    @classmethod
    def sync_deal_provider(cls):
        """
        同步订单中
        :return:
        """
        deal_id = 1
        provider_log = get_db("business").provider_log.find_one()
        if provider_log:
            deal_id = provider_log.get("deal_id")
        deals = Deal.objects(deal_id__gt=deal_id, is_verified=True).order_by("deal_id").limit(1200)
        provider_list = []
        plus_client_list = []
        cur_deal_id = deal_id
        for deal in deals:
            cur_deal_id = deal.deal_id
            provider_dict = dict(
                create_time=datetime.datetime.utcnow()
            )
            provider_dict_inc = dict(
                deal_count=1,
                use_allowance=Decimal128(deal.piece_size),
                storage_price_per_epoch=Decimal128(deal.storage_price_per_epoch)
            )
            plus_client_dict_inc = dict(
                use_allowance=Decimal128(deal.piece_size),
                deal_count=1,
            )
            plus_client_dict = {}
            wallets = Wallets.objects(address=deal.client).first()
            if wallets:
                plus_client_dict["id_address"] = wallets.id
            plus_client = {"$inc": plus_client_dict_inc,
                           "$addToSet": {"providers": deal.provider}}
            if plus_client_dict:
                plus_client["$set"] = plus_client_dict
            plus_client_list.append(UpdateOne({"address": deal.client},
                                              plus_client,
                                              upsert=True))
            provider_list.append(UpdateOne({"miner_no": deal.provider},
                                           {"$set": provider_dict,
                                            "$inc": provider_dict_inc,
                                            "$addToSet": {"clients": deal.client}},
                                           upsert=True))

        if plus_client_list:
            get_db("business").plus_client.bulk_write(plus_client_list)
        if provider_list:
            get_db("business").provider.bulk_write(provider_list)
        get_db("business").provider_log.delete_many({})
        get_db("business").provider_log.insert_one({"deal_id": cur_deal_id})
        cls._repair_provider()
        return cur_deal_id

    @classmethod
    def _repair_provider(cls):
        """
        统计数量
        :return:
        """
        plus_client_list = []
        clients = PlusClient.objects().aggregate([{"$project": {"address": 1, "count": {"$size": "$providers"}}}])
        for client in clients:
            plus_client_list.append(
                UpdateOne({"address": client["address"]},
                          {"$set": {"provider_count": client["count"]}},
                          ))
        provider_list = []
        providers = Provider.objects().aggregate([{"$project": {"miner_no": 1, "count": {"$size": "$clients"}}}])
        for provider in providers:
            provider_list.append(
                UpdateOne({"miner_no": provider["miner_no"]},
                          {"$set": {"client_count": provider["count"]}},
                          ))

        if plus_client_list:
            get_db("business").plus_client.bulk_write(plus_client_list)
        if provider_list:
            get_db("business").provider.bulk_write(provider_list)

    @classmethod
    def sync_datacap_stats(cls, date_str=None):
        now_date = datetime.datetime.now().date()
        if date_str:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

        else:
            day_date = "2020-10-15"
            datacap_day = DatacapDay.objects().order_by("-date").first()
            if datacap_day:
                day_date = datacap_day.date
            date = (datetime.datetime.strptime(day_date, "%Y-%m-%d") + datetime.timedelta(days=1)).date()
            if date > now_date - datetime.timedelta(days=1):
                return
            date_str = date.strftime("%Y-%m-%d")
        # deal_gas_by_t = Decimal128("0")
        # overview_stat = bbhe_sdk.BbheBase().get_overview_stat(date_str)
        # if overview_stat.get("code") == 200:
        #     deal_gas_by_t = Decimal128(_d(overview_stat.get("data").get("orderGasByT", 0) * (10**18)))
        # 当天的最后一个高度数据
        height = datetime_to_height(date)
        client_count = len(Deal.objects(height__gte=height, height__lt=height + 2880, is_verified=True).distinct(
            "client"))
        provider_count = len(Deal.objects(height__gte=height, height__lt=height + 2880, is_verified=True).distinct(
            "provider"))
        deal = Deal.objects(height__gte=height, height__lt=height + 2880, is_verified=True).aggregate([
            {"$group": {"_id": 0,
                        "use_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        deal = list(deal)
        deals = deal[0] if deal else {}
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
        deal_gas_by_t = total_gas / (deals.get("use_size") / _d(1024 ** 4))
        DatacapDay.objects(date=date_str).upsert_one(client_count=client_count, provider_count=provider_count,
                                                     use_size=_d(deals.get("use_size", "0")),
                                                     deal_count=deals.get("count", 0), deal_gas_by_t=deal_gas_by_t)


    @classmethod
    def get_notaries_list(cls, key_words, page_index=1, page_size=20):
        """
        获取公正人列表
        :param key_words:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q(id_address__ne=None)
        if key_words:
            query = Q(id_address=key_words) | Q(address=key_words)
        query = Notaries.objects(query)
        result = mongo_paginator(query, page_index, page_size)
        data = []
        for info in result['objects']:
            tmp = info.to_dict(exclude_fields=("create_time",))
            data.append(tmp)
        result["objects"] = data
        return result

    @classmethod
    def get_plus_client_list(cls, key_words, page_index=1, page_size=20):
        """
        客户列表
        :param key_words:
        :param page_index:
        :param page_size:
        :return:
        """

        query = Q(id_address__ne=None)
        # query = Q()
        if key_words:
            query &= (Q(id_address=key_words) | Q(address=key_words) | Q(notaries__id_address=key_words) | Q(
                notaries__address=key_words) | Q(providers=key_words))

        query = PlusClient.objects(query).order_by("-deal_count")
        result = mongo_paginator(query, page_index, page_size)
        data = []
        for info in result['objects']:
            tmp = info.to_dict(exclude_fields=("create_time",))
            data.append(tmp)
        result["objects"] = data
        return result

    @classmethod
    def get_provider_list(cls, key_words, page_index=1, page_size=20):
        """
        存储提供商列表
        :param key_words:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if key_words:
            query = Q(miner_no=key_words) | Q(clients=key_words)

        query = Provider.objects(query)
        result = mongo_paginator(query, page_index, page_size)
        data = []
        for info in result['objects']:
            tmp = info.to_dict(exclude_fields=("create_time",))
            tmp["avg_price"] = (tmp["storage_price_per_epoch"] / tmp["deal_count"]).quantize(decimal.Decimal("0"),
                                                                                             rounding=decimal.ROUND_HALF_UP)
            data.append(tmp)
        result["objects"] = data
        return result

    @classmethod
    @cache.cached(timeout=600)
    def get_datacap_dashboard(cls):
        # 公证人数据
        notaries = Notaries.objects(id_address__ne=None).aggregate([
            {"$group": {"_id": 0,
                        "total": {"$sum": "$granted_allowance"},
                        "allocated": {"$sum": "$allocated_allowance"},
                        "count": {"$sum": 1}}},
        ])
        notaries = list(notaries)
        notaries_data = notaries[0] if notaries else {}
        # 分布
        notaries_distributed = Notaries.objects(id_address__ne=None).aggregate([
            {"$group": {"_id": "$region",
                        "total": {"$sum": "$granted_allowance"}}},
        ])
        notaries_distributed = {d.get("_id").replace("-", "_"): bson_to_decimal(d.get("total")) for d in
                                notaries_distributed}
        # 实际链上数据
        provider = Provider.objects().aggregate([
            {"$group": {"_id": 0,
                        "total_deal_count": {"$sum": "$deal_count"},
                        "use": {"$sum": "$use_allowance"},
                        "count": {"$sum": 1}}},
        ])
        provider = list(provider)
        provider_data = provider[0] if provider else {}
        # total_client
        total_client = PlusClient.objects(Q(id_address__ne=None)).count()
        # data_size
        deal_data = Deal.objects(is_verified=True).aggregate([
            {"$group": {"_id": 0,
                        "data_size": {"$sum": "$piece_size"},
                        "count": {"$sum": 1}}},
        ])
        deal_data = list(deal_data)
        deal_data = deal_data[0] if deal_data else {}
        result = {
            "notaries_count": notaries_data.get("count"),
            "client_count": total_client,
            "provider_count": provider_data.get("count"),
            "deal_count": deal_data.get("count"),
            "data_size": bson_to_decimal(deal_data.get("data_size")),
            "notaries_datacap": {
                "total": bson_to_decimal(notaries_data.get("total")),
                "allocated": bson_to_decimal(notaries_data.get("allocated")),
            },
            "distributed": notaries_distributed
        }
        return result

    @classmethod
    def get_datacap_stats(cls, stats_type):
        """
        dataca每天客户和存储提供商之间活跃统计
        :return:
        """
        limit = int(stats_type[0:stats_type.find("d")])
        datacap_days = DatacapDay.objects().order_by("-date").limit(limit)
        data = []
        for info in datacap_days:
            tmp = info.to_dict()
            data.append(tmp)
        if limit==90:
            data = data[::3]
        return data
