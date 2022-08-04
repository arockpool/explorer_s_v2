import mongoengine.fields as fields
from base.db.mongo import MongoBaseModel, Decimal2Field


class Blocks(MongoBaseModel):
    block_hash = fields.StringField(help_text='消息hash')
    miner_id = fields.StringField(help_text='矿工id')
    msg_count = fields.LongField(help_text='消息数量')
    win_count = fields.LongField(help_text='赢票数量', default=0)
    block_reward = Decimal2Field(help_text='区块奖励', precision=0)
    gas_reward = Decimal2Field(help_text='gas奖励', precision=0, default=0)
    penalty = Decimal2Field(help_text='惩罚', precision=0, default=0)
    parent_weight = fields.StringField(help_text='父区块权重')
    parents = fields.ListField(fields.StringField(), help_text='父区块ID')
    # size = fields.LongField(help_text='区块大小')
    height = fields.IntField()
    height_time = fields.DateTimeField()

    meta = {
        "db_alias": "base",
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
            {"fields": ("block_hash",)},
            {"fields": ("miner_id", "-height")},
        ]
    }


class Tipset(MongoBaseModel):
    total_msg_count = fields.LongField(help_text='消息数量', default=0)
    total_win_count = fields.LongField(help_text='赢票数量', default=0)
    total_reward = Decimal2Field(help_text='奖励', precision=0, default=0)
    height = fields.IntField()
    height_time = fields.DateTimeField()
    meta = {
        "db_alias": "base",
        "strict": False,
        "index_background": True,
        'indexes': [
            {"fields": ("-height",)},
        ]
    }


class BlockMessages(MongoBaseModel):
    """
    区块和消息的映射关系
    """
    height = fields.LongField()
    height_time = fields.DateTimeField()
    block = fields.StringField()
    messages = fields.ListField()
    meta = {
        "db_alias": "base",
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
            {"fields": ("block",)},
            {"fields": ("messages",)},
        ]
    }
