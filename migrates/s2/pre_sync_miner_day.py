import datetime, os, sys
from explorer.models.message import MigrateLog
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(BASE_DIR))
from explorer_s_common import inner_server
from bson.decimal128 import Decimal128
from pymongo import UpdateOne
from base.utils.fil import _d
from mongoengine.connection import get_db
import pandas as pd


class PreProcess:
    def __init__(self, app):
        self.process_data(app)

    @classmethod
    def process_data(cls, app):
        # now_day = datetime.datetime.now().strftime("%Y-%m-%d")
        # miner_day = MinerDay.objects().order_by("date").first()
        # if miner_day:
        #     now_day = miner_day.date
        # date = datetime.datetime.strptime(now_day, "%Y-%m-%d")
        # while date > datetime.datetime.strptime("2021-01-03", "%Y-%m-%d"):
        #     date = date - datetime.timedelta(days=1)
        now_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        tables = pd.date_range('2021-11-01', now_str, freq='D').strftime("%Y-%m-%d").tolist()
        tables.reverse()
        for date_str in tables:
            print(date_str)
            cls.get_history_miner_value(date_str)

    @classmethod
    def get_history_miner_value(cls, date):
        """
        获得历史的矿工产出记录
        :param date: 历史数据日期
        :return:
        """
        i = 1
        miner_days = []
        while True:
            result = inner_server.get_miner_day_records({"page_size": 100, "page_index": i, "date": date})
            # 保存数据
            for miner_info in result['data']['objs']:
                miner_day = UpdateOne({"miner_no": miner_info.get("miner_no"), "date": date},
                                      {"$set": dict(
                                          worker_balance=Decimal128(_d(miner_info.get("worker_balance"))),
                                          post_balance=Decimal128(_d(miner_info.get("poster_balance"))),
                                          owner_balance=Decimal128(_d(miner_info.get("owner_balance")))
                                      )},)
                miner_days.append(miner_day)
            if i >= result['data']["total_page"]:
                break
            else:
                i += 1
        if miner_days:
            get_db("business").miner_day.bulk_write(miner_days)
