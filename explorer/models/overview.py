import mongoengine.fields as fields
from base.db.mongo import MongoBaseModel, Decimal2Field


class Overview(MongoBaseModel):
    power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="原值算力", precision=0, default=0)
    active_miner_count = fields.LongField(help_text="活跃矿工数")
    total_account = fields.LongField(help_text="钱包地址数")
    pledge_per_sector = Decimal2Field(help_text="每个扇区的质押值", precision=0, default=0)
    circulating_supply = Decimal2Field(help_text="流通量", precision=0, default=0)
    burnt_supply = Decimal2Field(help_text="销毁量", precision=0, default=0)
    base_fee = Decimal2Field(help_text="基础费率", precision=0, default=0)
    msg_count = fields.LongField(help_text="24小时消息数")
    reward = Decimal2Field(help_text="全网总区块奖励", precision=0, default=0)
    total_pledge = Decimal2Field(help_text="总质押", precision=0, default=0)
    block_count = fields.LongField(help_text="当前高度区块出块数")
    block_reward = Decimal2Field(help_text="当前高度区块奖励", precision=0, default=0)
    # avg_reward_24 = Decimal2Field(help_text="24小的产出效率")
    # avg_block_reward_24 = Decimal2Field(help_text="24小的出块奖励")
    # avg_tipset_blocks = Decimal2Field(help_text="平均区块高度")
    height = fields.IntField()
    height_time = fields.DateTimeField()
    synced_at_str = fields.DateTimeField()
    synced_at = fields.IntField()
    meta = {
        "db_alias": "base",
        "index_background": True,
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
        ]
    }


class OverviewStat(MongoBaseModel):
    power = Decimal2Field(help_text="总算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    increase_power = Decimal2Field(help_text='算力增速度', dprecision=0, default=0)
    increase_power_offset = Decimal2Field(help_text='算力增量', precision=0, default=0)
    increase_power_loss = Decimal2Field(help_text='算力损失', precision=0, default=0)
    avg_pledge = Decimal2Field(help_text="质押值", precision=0, default=0)
    avg_reward = Decimal2Field(help_text="产出效率", precision=0, default=0)
    height = fields.IntField(unique=True)
    height_time = fields.DateTimeField()
    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("-height_time",)},
        ]
    }


class OverviewDay(MongoBaseModel):
    date = fields.StringField()
    power = Decimal2Field(help_text="总算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    active_miner_count = fields.LongField(help_text="活跃矿工数")
    total_account = fields.LongField(help_text="钱包地址数")
    circulating_supply = Decimal2Field(help_text="流通量", precision=0, default=0)
    burnt_supply = Decimal2Field(help_text="销毁量", precision=0, default=0)
    reward = Decimal2Field(help_text="全网总区块奖励", precision=0, default=0)
    total_pledge = Decimal2Field(help_text="总质押", precision=0, default=0)
    price = Decimal2Field(help_text='fil价格', default=0)
    avg_pledge = Decimal2Field(help_text="平均质押值", precision=0, default=0)
    avg_reward = Decimal2Field(help_text="产出效率", precision=0, default=0)
    increase_power = Decimal2Field(help_text='算力增速度', precision=0, default=0)
    increase_power_offset = Decimal2Field(help_text='算力增量', precision=0, default=0)
    increase_power_loss = Decimal2Field(help_text='算力损失', precision=0, default=0)
    create_gas_32 = Decimal2Field(help_text='32G生产gas', precision=0, default=0)
    create_gas_64 = Decimal2Field(help_text='64G生产gas', precision=0, default=0)
    keep_gas_32 = Decimal2Field(help_text='32G维护gas', precision=0, default=0)
    keep_gas_64 = Decimal2Field(help_text='64G维护gas', precision=0, default=0)
    rmd_avg_pledge = Decimal2Field(help_text="平均质押值", precision=0, default=0)
    rmd_avg_reward = Decimal2Field(help_text="产出效率", precision=0, default=0)
    rmd_create_gas_32 = Decimal2Field(help_text='32G生产gas', precision=0, default=0)
    rmd_create_gas_64 = Decimal2Field(help_text='64G生产gas', precision=0, default=0)
    rmd_keep_gas_32 = Decimal2Field(help_text='32G维护gas', precision=0, default=0)
    rmd_keep_gas_64 = Decimal2Field(help_text='64G维护gas', precision=0, default=0)
    meta = {
        "db_alias": "business",
        "strict": False,
        "index_background": True,
        'indexes': [
            {"fields": ("-date",)},
        ]
    }
