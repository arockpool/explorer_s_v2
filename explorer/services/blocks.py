import datetime
from base.utils.fil import _d, height_to_datetime, datetime_to_height,bson_to_decimal
from explorer.models.blocks import Blocks, Tipset
from base.utils.paginator import mongo_paginator


class BlocksService(object):
    """
    区块服务
    """

    @classmethod
    def get_reward_by_range_height(cls, start_height, end_height):
        """
        获取奖励通过高度范围
        :param start_height:
        :param end_height:
        :return:
        """
        block_reward = Blocks.objects(height__gt=start_height, height__lte=end_height).sum("block_reward")
        gas_reward = Blocks.objects(height__gt=start_height, height__lte=end_height).sum("gas_reward")
        return bson_to_decimal(block_reward) + bson_to_decimal(gas_reward)


    @classmethod
    def get_blocks_by_range_height(cls, start_height=None, end_height=None):
        """
        获取出块信息
        :param start_height:
        :param end_height:
        :return:
        """
        pipeline = [
            {"$group": {"_id": "$miner_id",
                        "sum_win_count": {"$sum": "$win_count"},
                        "sum_block_reward": {"$sum": "$block_reward"},
                        "sum_gas_reward": {"$sum": "$gas_reward"},
                        "block_count": {"$sum": 1},
                        }}
        ]
        obj_dict = {}
        if start_height and end_height:
            obj_dict["height__gte"] = start_height
            obj_dict["height__lt"] = end_height
        block = Blocks.objects(**obj_dict).aggregate(pipeline)
        return block

    @classmethod
    def get_tipset_block_count(cls, start_height, end_height=None):
        """获取奖出块数量"""
        query_dict={}
        if start_height:
            query_dict["height__gte"]= start_height
        if end_height:
            query_dict["height__lte"] = end_height
        return Blocks.objects(**query_dict).count()


    @classmethod
    def get_tipsets(cls, page_index=1, page_size=20):
        """
        获取高度区块列表
        :param page_index:
        :param page_size:
        :return:
        """

        query = Tipset.objects().order_by("-height")
        result = mongo_paginator(query, page_index, page_size)
        heights = [info.height for info in result['objects']]
        now = datetime.datetime.now()
        blocks = Blocks.objects(height__in=heights).all()
        print(datetime.datetime.now() - now)

        block_dict = {}
        for block in blocks:
            block_dict.setdefault(block.height, [])
            block_dict[block.height].append(block.to_dict(only_fields=("block_hash", "miner_id", "msg_count",
                                                                       "block_reward")))
        data = []
        for info in result['objects']:
            tmp = info.to_dict(only_fields=("height", "height_time"))
            tmp["blocks"] = block_dict.get(info.height, [])
            data.append(tmp)
        result["objects"] = data
        return result

    @classmethod
    def get_tipset_info(cls, height):
        """
        获取高度区块详情
        :param height:
        :return:
        """

        tipset = Tipset.objects(height=height).first()
        data = tipset.to_dict()
        blocks = Blocks.objects(height=height).all()
        data["blocks"] = [block.to_dict() for block in blocks]
        return data

    @classmethod
    def get_block_info(cls, block_hash):
        """
        获取区块详情
        :param block_hash:
        :return:
        """

        block = Blocks.objects(block_hash=block_hash).first()
        return block.to_dict()

    @classmethod
    def get_blocks(cls, miner_id, start_date=None, end_date=None, page_index=1, page_size=20):
        """
        获取区块列表
        :param miner_id:
        :param start_date:
        :param end_date:
        :param page_index:
        :param page_size:
        :return:
        """
        query_dict = {"miner_id": miner_id}
        if start_date:
            start_height = datetime_to_height(datetime.datetime.strptime(start_date + ' 00:00:00', "%Y-%m-%d %H:%M:%S"))
            query_dict["height__gte"] = start_height
        if end_date:
            end_height = datetime_to_height(datetime.datetime.strptime(end_date + ' 23:59:59', "%Y-%m-%d %H:%M:%S"))
            query_dict["height__lte"] = end_height
        query = Blocks.objects(**query_dict).order_by("-height")
        result = mongo_paginator(query, page_index, page_size)
        result['objects'] = [info.to_dict(
            only_fields=("block_hash", "height", "win_count", "block_reward", "gas_reward", "msg_count", "height_time"))
            for info in result['objects']]
        return result

    @classmethod
    def get_is_tipset(cls, value):
        """
        判断是否是tipset
        :param value:
        :return:
        """
        return Tipset.objects(height=value).first()

    @classmethod
    def get_is_block(cls, value):
        """
        判断是否是block
        :param value:
        :return:
        """
        return Blocks.objects(block_hash=value).first()
