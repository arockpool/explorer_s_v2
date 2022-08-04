import datetime
from mongoengine import Q
from base.utils.fil import _d, height_to_datetime, datetime_to_height
from explorer.models.wallets import Wallets, WalletRecords
from base.utils.paginator import mongo_paginator
from bson.decimal128 import Decimal128


class WalletsService(object):
    """
    钱包服务
    """

    @classmethod
    def get_wallets_list(cls, wallet_type=None, id_address_list=[], page_index=1, page_size=20):
        """
        获取钱包列表
        :param wallet_type:
        :param id_address_list:
        :param page_index:
        :param page_size:
        :return:
        """
        query = Q()
        if wallet_type:
            query = Q(wallet_type__endswith=wallet_type)
        else:
            query = Q(wallet_type__in=["fil/5/multisig","fil/5/storageminer","fil/5/account"])
        # else:
        #     query = Q(wallet_type__endswith="multisig")|Q(wallet_type__endswith="storageminer")|Q(wallet_type__endswith ="account")
        if id_address_list:
            query = Q(id__in=id_address_list)|Q(address__in=id_address_list)
        query_d = Wallets.objects(query).order_by("-value")
        result = mongo_paginator(query_d, page_index, page_size)
        result['objects'] = [info.to_dict(only_fields=("id", "address", "value", "wallet_type", "create_height_time",
                                                       "update_height_time")) for info in result['objects']]

        return result

    @classmethod
    def get_wallet_info(cls, id_or_address):
        """
        获取高度区块详情
        :param id_or_address:
        :return:
        """

        wallet = Wallets.objects(Q(id=id_or_address)|Q(address=id_or_address)).first()
        data = wallet.to_dict()
        if data["start_epoch"]:
            data["start_epoch_time"] = height_to_datetime(data["start_epoch"])
        if data["unlock_duration"]:
            try:
                data["unlock_duration_time"] = height_to_datetime(data["unlock_duration"])
            except:
                data["unlock_duration_time"] = "Invalid date"
        return data

    @classmethod
    def get_wallet_record(cls, id_or_address):
        """
        获取高度区块详情
        :param id_or_address:
        :return:
        """

        wallet_records = WalletRecords.objects(Q(address_id=id_or_address)|Q(address=id_or_address)).order_by("-height")[:100]
        result = [info.to_dict(only_fields=("value", "height_time")) for info in wallet_records]
        result.reverse()
        return result

    @classmethod
    def get_is_wallet(cls, value):
        """
        判断是否是wallet
        :param value:
        :return:
        """
        return Wallets.objects((Q(id=value) | Q(address=value)) & Q(wallet_type__nin=["fil/5/storageminer",
                                                                                      "fil/6/storageminer"])).first()

    @classmethod
    def get_wallet_address_change(cls, wallet_address, balance_value, height):
        """
        查询钱包的具体改变值
        :return:
        """
        record = WalletRecords.objects(address=wallet_address, height__gte=height,
                                       value__gte=Decimal128(balance_value)).order_by("-height").first()
        return [record.value] if record else []

    @classmethod
    def get_is_all_wallet(cls, value):
        """
        判断是否是所有类型钱包wallet
        :param value:
        :return:
        """
        return Wallets.objects(Q(id=value) | Q(address=value)).first()



