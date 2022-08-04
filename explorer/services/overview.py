import datetime
import decimal

from pymongo import ReplaceOne
from base.third.gateio_sdk import GateioBase
from base.third.fxh_sdk import FxhBase
from base.third.other_sdk import RMDine
from base.third.bbhe_sdk import BbheBase
from base.utils.fil import _d, height_to_datetime, datetime_to_height, local2utc, bson_to_decimal
from base.flask_ext import cache
from explorer.models.overview import Overview, OverviewDay, OverviewStat
from explorer.models.blocks import Tipset
from explorer.services.blocks import BlocksService
from explorer.services.message import TipsetGasService, TipsetService
from explorer.services.miner import MinerService
from mongoengine.connection import get_db
from base.utils.paginator import mongo_paginator
from bson.decimal128 import Decimal128


class OverviewDayService(object):
    """
    全网每天的数据表
    """

    @classmethod
    def sync_overview_day(cls, date_str=None):
        """
        同步全网每天的数据
        :param date_str:
        :return:
        """
        now_date = datetime.datetime.now().date()
        if date_str:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

        else:
            day_date = "2020-10-15"
            over_view_day = OverviewDay.objects().order_by("-date").first()
            if over_view_day:
                day_date = over_view_day.date
            date = (datetime.datetime.strptime(day_date, "%Y-%m-%d") + datetime.timedelta(days=1)).date()
            if date > now_date - datetime.timedelta(days=1):
                return
            date_str = date.strftime("%Y-%m-%d")
        is_today = True if date == now_date - datetime.timedelta(days=1) else False
        # 当天的最后一个高度数据
        height = datetime_to_height(date) + 2880 - 1
        over_view = Overview.objects(height__lte=height).order_by("-height").first()
        over_view_day = dict(
            power=Decimal128(over_view.power),
            raw_power=Decimal128(over_view.raw_power),
            active_miner_count=over_view.active_miner_count,
            total_account=over_view.total_account,
            circulating_supply=Decimal128(over_view.circulating_supply),
            burnt_supply=Decimal128(over_view.burnt_supply),
            reward=Decimal128(over_view.reward),
            total_pledge=Decimal128(over_view.total_pledge),
            rmd_avg_pledge=Decimal128("0"),
            rmd_avg_reward=Decimal128("0"),
            rmd_create_gas_32=Decimal128("0"),
            rmd_create_gas_64=Decimal128("0"),
            rmd_keep_gas_32=Decimal128("0"),
            rmd_keep_gas_64=Decimal128("0")
        )
        # 全网算力增加量
        over_view_yesterday = Overview.objects(height__lte=height - 2880).order_by("-height").first()
        over_view_day["increase_power"] = Decimal128(
            TipsetGasService.get_overview_increase_power(start_height=height - 2880,
                                                         end_height=height))
        over_view_day["increase_power_offset"] = Decimal128(over_view.power - over_view_yesterday.power)
        increase_power_loss = None
        if is_today:
            increase_power_loss = Decimal128(MinerService.get_increase_power_loss())
        over_view_day['increase_power_loss'] = increase_power_loss
        # 全网产出效率
        over_view_day["avg_reward"] = Decimal128(cls.get_avg_reward(height - 2880, height)[0])
        # 全网平均质押
        pledge_per_sector = Overview.objects(height__gt=height - 2880, height__lte=height).average("pledge_per_sector")
        over_view_day["avg_pledge"] = Decimal128(bson_to_decimal(pledge_per_sector) * 32)
        # 价格
        price = None
        # if is_today:
        #     price = Decimal128(cls.get_price()[0])
        over_view_day["price"] = price
        # 成本数据
        over_view_day.update(TipsetGasService.get_gas_cost_by_range_height(height - 2880, height))
        OverviewDay.objects(date=date_str).upsert_one(**over_view_day)

    @classmethod
    def get_avg_reward(cls, start_height, end_height):
        """
        获取产出效率
        :param start_height:
        :param end_height:
        :return:
        """
        # 奖励
        tipset_sum_reward = BlocksService.get_reward_by_range_height(start_height, end_height)

        avg_power = Overview.objects(height__gt=start_height, height__lte=end_height).average("power")
        avg_power = bson_to_decimal(avg_power) / _d(1024 ** 4)

        return tipset_sum_reward / avg_power if avg_power else _d(0), tipset_sum_reward

    @classmethod
    def get_price(cls):
        """
        获取价格和价格变化量
        :return:
        """
        price = 0
        price_change = 0
        gateio_result = GateioBase().get_ticker()
        if gateio_result:
            price = _d(gateio_result.get('last', '0'))
            price_change = _d(gateio_result.get('percentChange', '0'))

        if price == 0:
            fxh_result = FxhBase().get_ticker()
            if fxh_result:
                temp = [x for x in fxh_result if x['symbol'] == 'FIL'][0]
                price = _d(temp.get('price_usd', '0'))
                price_change = _d(temp.get('percent_change_24h', '0'))
        return _d(price), price_change

    @classmethod
    def sync_overview_day_rmd(cls, date_str=None):
        """
        同步全网每天rmd产出数据
        :param date_str:
        :return:
        """
        # result = RMDine().get_fil_overview()
        # data = result.get("datas")
        # if data:
        #     OverviewDay.objects(date=date_str).update(
        #         rmd_avg_pledge=Decimal128(_d(data["avg_pledge"]) * _d(10 ** 18)),
        #         rmd_avg_reward=Decimal128(_d(data["pool_unit_t_reward"]) * _d(10 ** 18)),
        #         rmd_create_gas_32=Decimal128(_d(data["create_cost_gas_per_t"]) * _d(10 ** 18)),
        #         rmd_create_gas_64=Decimal128(_d(data["create_cost_gas_per_t_64"]) * _d(10 ** 18)),
        #         rmd_keep_gas_32=Decimal128(_d(data["keep_cost_gas_per_t"]) * _d(10 ** 18)),
        #         rmd_keep_gas_64=Decimal128(_d(data["keep_cost_gas_per_t_64"]) * _d(10 ** 18))
        #     )
        if not date_str:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
        # 获取昨日信息，没有则取前天
        yesterday_info = BbheBase().get_overview_stat(date=date_str)
        if yesterday_info['code'] != 200 or (not yesterday_info['data']) or (
                yesterday_info['data'].get('poolUnitTReward', 0) <= 0) or (
                yesterday_info['data'].get('unitTReward', 0) <= 0):
            date_str = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
            yesterday_info = BbheBase().get_overview_stat(date=date_str)

        if yesterday_info['code'] != 200 or (not yesterday_info['data']):
            return
        avg_pledge = _d(yesterday_info['data'].get('unitTPackingPledgeFee', '8.2410'))
        create_gas_32 = max(_d(yesterday_info['data'].get('unitTPackingGasFee32', '0.0001')), _d(0.0001))
        create_gas_64 = max(_d(yesterday_info['data'].get('unitTPackingGasFee64', '0.0001')), _d(0.0001))
        keep_gas_32 = max(_d(yesterday_info['data'].get('unitTMaintainGasFee32', '0.0001')), _d(0.0001))
        keep_gas_64 = max(_d(yesterday_info['data'].get('unitTMaintainGasFee64', '0.0001')), _d(0.0001))
        pool_unit_t_reward = yesterday_info['data'].get('poolUnitTReward', '0')
        unit_t_reward = _d(yesterday_info['data'].get('unitTReward', '0'))

        OverviewDay.objects(date=date_str).update(
            rmd_avg_pledge=Decimal128(avg_pledge * _d(10 ** 18)),
            rmd_avg_reward=Decimal128(unit_t_reward * _d(10 ** 18)),
            rmd_create_gas_32=Decimal128(create_gas_32 * _d(10 ** 18)),
            rmd_create_gas_64=Decimal128(create_gas_64 * _d(10 ** 18)),
            rmd_keep_gas_32=Decimal128(keep_gas_32 * _d(10 ** 18)),
            rmd_keep_gas_64=Decimal128(keep_gas_64 * _d(10 ** 18))
        )

    @classmethod
    def sync_overview_stat(cls):
        """
        同步全网每个高度的的数据统计
        :return:
        """
        height = 1
        last_power = 0
        over_view_stat = OverviewStat.objects().order_by("-height").first()
        if over_view_stat:
            height = over_view_stat.height
            last_power = over_view_stat.power
        now_height = datetime_to_height(datetime.datetime.now()) - 200  # 修补block需要150个高度
        query_dict = {"height__gt": height, "height__lte": now_height}
        over_views = Overview.objects(**query_dict).order_by("height").no_dereference().limit(1200)
        over_view_stats = []
        for over_view in over_views:
            over_view_stat_dict = dict(
                power=Decimal128(over_view.power),
                raw_power=Decimal128(over_view.raw_power),
                increase_power_offset=Decimal128(over_view.power - last_power),
                increase_power=Decimal128(TipsetGasService.get_overview_increase_power(over_view.height)),
                increase_power_loss=Decimal128("0"),  # 每个高度的算力损失暂时为0
                height=over_view.height,
                height_time=over_view.height_time,
                avg_pledge=Decimal128(over_view.pledge_per_sector * 32),
                avg_reward=Decimal128(cls.get_avg_reward(over_view.height - 2880, over_view.height)[0]),
            )
            last_power = over_view.power
            over_view_stats.append(ReplaceOne({"height": over_view.height}, over_view_stat_dict, upsert=True))
        if over_view_stats:
            get_db("business").overview_stat.bulk_write(over_view_stats)
        return height


class OverviewService(object):
    """
    全网数据服务
    """
    @classmethod
    def get_overview(cls):
        overview_dict = cls._get_overview()
        overview = Overview.objects().order_by("-height").only("height").first()
        overview_dict["height"] = overview.height
        return overview_dict

    @classmethod
    @cache.cached(timeout=600)
    def _get_overview(cls):
        """
        获取全网实时数据
        :return:
        """
        overview = Overview.objects().order_by("-height").first()
        print('overview',overview)
        overview_dict = overview.to_dict()
        start_height = overview.height - 2880
        overview_dict["avg_reward"], overview_dict["reward_24"] = OverviewDayService.get_avg_reward(start_height,
                                                                                                    overview.height)
        overview_dict["avg_tipset_blocks"] = Overview.objects(height__gte=start_height,
                                                              height__lte=overview.height).average("block_count")
        # 24小时消息数量
        overview_dict["msg_count"] = Overview.objects(height__gte=start_height,
                                                      height__lte=overview.height).sum("msg_count")
        # 24小时算力增加量
        start_overview = Overview.objects(height__lte=start_height).order_by("-height").first()
        overview_dict["increase_power_offset"] = overview_dict["power"] - start_overview.power
        # 特殊处理 base_fee 和 pledge_per_sector
        overview_dict["pledge"] = overview.pledge_per_sector * 32 / 10 ** 18
        if not overview.pledge_per_sector:
            pledge_per_sector = Overview.objects(pledge_per_sector__ne=0).order_by("-height").first().pledge_per_sector
            overview_dict["pledge"] = pledge_per_sector * 32 / 10 ** 18
        if not overview.base_fee:
            overview_dict["base_fee"] = Overview.objects(base_fee__ne=0).order_by("-height").first().base_fee
        return overview_dict

    @classmethod
    @cache.cached(timeout=600)
    def get_overview_stat(cls):
        """
        获取全网实时数据统计
        :return:
        """
        overview_stat = OverviewStat.objects().order_by("-height").first()
        # 当前最新高度生产Gas
        create_gas_32, create_gas_64 = TipsetService.get_first_create_gas()
        print(overview_stat)
        print('gas32',create_gas_32,'gas64',create_gas_64)
        overview_stat_dict = overview_stat.to_dict()
        overview_stat_dict["create_gas_32"] = create_gas_32
        overview_stat_dict["create_gas_64"] = create_gas_64
        # day_height = datetime_to_height(datetime.datetime.now().date())
        now_height = overview_stat.height
        day_height = now_height - 2880
        # 今日算力增速
        overview_stat_dict["increase_power"] = TipsetGasService.get_overview_increase_power(start_height=day_height,
                                                                                            end_height=now_height)
        # 今日算力增加增加量
        overview = Overview.objects(height__lte=day_height).order_by("-height").first()
        overview_stat_dict["increase_power_offset"] = overview_stat.power - overview.power
        # 今日算力损失
        overview_stat_dict["increase_power_loss"] = MinerService.get_increase_power_loss()
        return overview_stat_dict

    @classmethod
    def get_overview_stat_list(cls, date=None, height=None, page_index=1, page_size=20):
        """
        获取全网每个高度的数据
        :param date:
        :param height:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {}
        if date:
            start_date = datetime.datetime.strptime(date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
            end_date = datetime.datetime.strptime(date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
            query_dict["height_time__gte"] = local2utc(start_date)
            query_dict["height_time__lte"] = local2utc(end_date)
        if height:
            query_dict["height"] = height
        query = OverviewStat.objects(**query_dict).order_by("-height")
        result = mongo_paginator(query, page_index, page_size)
        result["objects"] = [info.to_dict(exclude_fields=["raw_power"]) for info in result['objects']]
        return result

    @classmethod
    def get_overview_day_list(cls, start_date=None, end_date=None, page_index=1, page_size=20):
        """
        获取全网按天的数据列表
        :param start_date:
        :param end_date:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {}
        if start_date:
            query_dict["date__gte"] = start_date
        if end_date:
            query_dict["date__lte"] = end_date
        if datetime.datetime.now().hour < 10:
            now_time = datetime.datetime.now()
            if end_date:
                now_time = datetime.datetime.now().strptime(end_date, "%Y-%m-%d")
            query_dict["date__lt"] = (now_time-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        query = OverviewDay.objects(**query_dict).order_by("-date")
        result = mongo_paginator(query, page_index, page_size)
        exclude_fields = ["active_miner_count", "total_account", "circulating_supply",
                          "reward", "total_pledge", "burnt_supply"]
        objects=[]
        for info in result['objects']:
            tmp = info.to_dict(exclude_fields=exclude_fields)
            if tmp["rmd_avg_reward"] <= decimal.Decimal("0"):
                tmp["rmd_avg_pledge"] = tmp["avg_pledge"]
                tmp["rmd_avg_reward"] = tmp["avg_reward"]
                tmp["rmd_create_gas_32"] = tmp["create_gas_32"]
                tmp["rmd_create_gas_64"] = tmp["create_gas_64"]
                tmp["rmd_keep_gas_32"] = tmp["keep_gas_32"]
                tmp["rmd_keep_gas_64"] = tmp["keep_gas_64"]
            objects.append(tmp)
        result["objects"]=objects
        return result

    @classmethod
    def _get_overview_power_trends(cls, days, spot=30):
        step = int(days / spot)
        now_date = datetime.datetime.today()
        ds_day = now_date - datetime.timedelta(days=days)
        ds_day_list = [ds_day + datetime.timedelta(days=step * x) for x in range(0, spot)]
        objs = OverviewDay.objects(date__gte=ds_day.strftime('%Y-%m-%d')).all()
        ds_day_dict = {}
        for obj in objs:
            ds_day_dict[obj.date] = {
                "power": obj.power,
                "increase_power": obj.increase_power,
                "increase_power_offset": obj.increase_power_offset,
                "increase_power_loss": obj.increase_power_loss,
                "date": obj.date
            }
        ds_day_list.reverse()
        ds_day_result = []
        for day_step in ds_day_list:
            if step > 1:
                power = ds_day_dict.get(day_step.strftime('%Y-%m-%d'), {}).get('power', 0)
                increase_power_offset = _d(0)
                increase_power = _d(0)
                increase_power_loss = _d(0)
                for day1 in [day_step - datetime.timedelta(days=x) for x in range(0, step)]:
                    day_ = day1.strftime('%Y-%m-%d')
                    increase_power_offset += ds_day_dict.get(day_, {}).get('increase_power_offset', _d(0))
                    increase_power += ds_day_dict.get(day_, {}).get('increase_power', _d(0))
                    increase_power_loss += ds_day_dict.get(day_, {}).get('increase_power_loss', 0)
                ds_day_result.append({
                    "power": power,
                    "increase_power_offset": increase_power_offset,
                    "increase_power": increase_power,
                    "increase_power_loss": increase_power_loss,
                    "date": day_step.strftime('%Y-%m-%d')
                })
            else:
                ds_day_result.append(ds_day_dict.get(day_step.strftime('%Y-%m-%d'), {
                    "power": 0,
                    "increase_power_offset": 0,
                    "increase_power": 0,
                    "increase_power_loss": 0,
                    "date": day_step.strftime('%Y-%m-%d')
                }))
        return ds_day_result

    @classmethod
    def get_overview_power_trends(cls, stats_type):
        """
        矿工的算力变化和出块统计
        """
        if stats_type == "30d":
            return cls._get_overview_power_trends(days=30)
        if stats_type == "180d":
            return cls._get_overview_power_trends(days=180)
        if stats_type == "360d":
            return cls._get_overview_power_trends(days=360)

    @classmethod
    def get_overview_stat_trends(cls, start_date, end_date, step):
        """
        获取gas趋势图
        :return:
        """
        start_height = datetime_to_height(start_date)
        end_height = datetime_to_height(end_date)
        data = OverviewStat.objects(height__gte=start_height, height__lte=end_height, height__mod=(step, 0)).all()
        result = [info.to_dict(only_fields=["height", "height_time", "avg_pledge", "avg_reward"])
                  for info in data]
        # 处理趋势图中为空的情况
        for d in result:
            if d["avg_pledge"] == decimal.Decimal("0"):
                stat = OverviewStat.objects(height__lt=d["height"], avg_pledge__ne=Decimal128("0")).order_by(
                    "-height").first()
                if stat:
                    d["avg_pledge"] = stat.avg_pledge
        return result

    @classmethod
    def _get_overview_day_list(cls, start_date=None, end_date=None):
        """
        获取全网按天的数据列表
        :param start_date:
        :param end_date:
        :return:
        """
        query_dict = {}
        if start_date:
            query_dict["date__gte"] = start_date
        if end_date:
            query_dict["date__lte"] = end_date
        query = OverviewDay.objects(**query_dict).order_by("-date")
        return query.all()
