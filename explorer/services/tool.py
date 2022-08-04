import datetime, math
from base.utils.fil import bson_to_decimal, utc2local
from mongoengine.connection import get_db
from explorer.services.message import MessageService
from explorer.models.wallets import WalletFlow
from base.utils.paginator import mongo_paginator


class WalletDirectionService(object):
    """
    钱包留向
    """

    @classmethod
    def get_wallet_flow_list(cls, address, direction, page_index=1, page_size=20):
        """
        查询转入转出地址
        :param address:
        :param direction:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {}
        if direction == "inflow":
            query_dict["to_address"] = address
        elif direction == "outflow":
            query_dict["from_address"] = address
        else:
            return {}
        query_d = WalletFlow.objects(**query_dict).order_by("-last_height")
        result = mongo_paginator(query_d, page_index, page_size)
        result['objects'] = [info.to_dict(exclude_fields=("msg_id_list",)) for info in result['objects']]
        for info in result['objects']:
            if direction == "inflow":
                info["msg_address"] = info["from_address"]
                info["msg_list"] = cls.get_wallet_message_list(info["from_address"], address, page_size=5).get("objects")
            if direction == "outflow":
                info["msg_address"] = info["to_address"]
                info["msg_list"] = cls.get_wallet_message_list(address, info["to_address"], page_size=5).get("objects")
            info.pop("from_address",None)
            info.pop("to_address", None)
        return result

    @classmethod
    def get_wallet_message_list(cls, from_address, to_address, page_index=1, page_size=20):
        """
        查询转入转出地址
        :param from_address:
        :param to_address:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {"msg_method_name": "Send"}
        if to_address:
            query_dict["msg_to"] = to_address
        if from_address:
            query_dict["msg_from"] = from_address
        table_dict, total_count = MessageService.get_message_count(query_dict)
        result = MessageService.get_page_offset_limit(table_dict, total_count, page_index=page_index,
                                                      page_size=page_size)
        data = []
        new_table_dict = result.pop("new_table_dict", {})
        print(new_table_dict)
        for table_name, offset_limit in new_table_dict.items():
            tmps = get_db("base")["messages@zone_" + table_name].find(query_dict).sort([("height", -1)]).skip(
                offset_limit[0]).limit(offset_limit[1])
            for info in tmps:
                tmp = {
                    "msg_cid": info["msg_cid"],
                    "height": info["height"],
                    "msg_value": bson_to_decimal(info["msg_value"]),
                    "height_time": utc2local(info["height_time"]),
                }
                data.append(tmp)
        result["objects"] = data
        return result
