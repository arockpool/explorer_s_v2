import datetime, os, sys
from explorer.models.message import MigrateLog
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(BASE_DIR))
from explorer.models.message import MessagesStat
from pymongo import UpdateOne
from base.utils.fil import _d,height_to_datetime
from mongoengine.connection import get_db

class PreProcess:

    def __init__(self, app):
        self.process_data(app)

    @classmethod
    def process_data(cls, app):
        """
        消息统计预先处理
        :return:
        """
        # 指定 202009 月份
        height = 19440
        end_height = 105840
        MessagesStat.objects(table_name="202009").delete()
        while height < end_height:
            height = cls.sync_messages_stat(height)
            print(height)

    @classmethod
    def sync_messages_stat(cls, height):
        end_height = height + 2880
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
        return end_height
