from base.db.mongo import MongoBaseModel, Decimal2Field
import mongoengine.fields as fields


class Miners(MongoBaseModel):
    miner = fields.StringField(unique=True, help_text="miner_id")
    address = fields.StringField()
    account_type = fields.StringField(help_text="类型")
    owner_id = fields.StringField()
    owner_address = fields.StringField()
    worker_id = fields.StringField()
    worker_address = fields.StringField()
    post_id = fields.StringField()
    post_address = fields.StringField()
    total_balance = Decimal2Field(help_text="余额", precision=0, default=0)
    available_balance = Decimal2Field(help_text="可用余额", precision=0, default=0)
    # pledge_balance = fields.FloatField(help_text="质押余额")
    precommit_deposits_balance = Decimal2Field(help_text="预交付保证金", precision=0, default=0)
    initial_pledge_balance = Decimal2Field(help_text="扇区抵押额", precision=0, default=0)
    locked_balance = Decimal2Field(help_text="锁仓额", precision=0, default=0)
    actual_power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="原值算力", precision=0, default=0)
    sector_size = fields.LongField(help_text="sector_size")
    sector_all = fields.LongField(help_text="总扇区数")
    sector_effect = fields.LongField(help_text="有效扇区数")
    sector_faults = fields.LongField(help_text="无效扇区数")
    sector_recovering = fields.LongField(help_text="恢复中扇区数")
    create_time = fields.DateTimeField(help_text="矿工创建时间")
    peer_id = fields.StringField(help_text="节点ID")
    total_reward = Decimal2Field(help_text="累计奖励", precision=0, default=0)
    total_block_count = fields.LongField(help_text="累计奖励", precision=0, default=0)
    total_win_count = fields.LongField(help_text="累计赢票数量", precision=0, default=0)
    all_actor_count = fields.IntField()
    all_burnt_supply_value = Decimal2Field(precision=0, default=0)
    multi_addrs = fields.ListField(fields.StringField(), help_text="节点多个地址")
    synced_at_str = fields.DateTimeField()
    synced_at = fields.IntField()

    meta = {
        "db_alias": "base",
        "strict": False,
        "index_background": True,
        'indexes': [
            {"fields": ("miner",)},
            {"fields": ("address",)},
            {"fields": ("actual_power",)},
            {"fields": ("sector_effect",)},
            {"fields": ("owner_address", "post_address")},
            {"fields": ("worker_address",)}
        ]
    }


class MinerHotHistory(MongoBaseModel):
    """矿工热表，只保留48小时数据"""
    miner_no = fields.StringField(help_text='矿工no', db_index=True)
    record_time = fields.DateTimeField(help_text='记录时间')
    raw_power = Decimal2Field(help_text='原值算力', precision=0, default=0)
    actual_power = Decimal2Field(help_text='有效算力', precision=0, default=0)
    sector_all = fields.LongField(help_text='总的扇区数', default=0)
    sector_effect = fields.LongField(help_text='有效的扇区数', default=0)
    sector_size = fields.LongField(help_text='扇区大小', default=0)

    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("miner_no",)},
        ]
    }


class MinerStat(MongoBaseModel):
    """
    24小时状态统计
    """
    miner_no = fields.StringField(unique=True, help_text="miner_no")
    sector_size = fields.LongField(help_text="sector_size")
    increase_power = Decimal2Field(help_text='24h新增算力(封装量)', precision=0, default=0)
    increase_power_offset = Decimal2Field(help_text='24h算力增速', precision=0, default=0)
    avg_reward = Decimal2Field(help_text='24h产出效率  atto FIL/TiB', precision=0, default=0)
    lucky = Decimal2Field(help_text='幸运值', precision=4, default=0)
    block_reward = Decimal2Field(help_text='出块奖励', precision=0, default=0)
    block_count = fields.IntField(help_text='出块数量', default=0)
    win_count = fields.IntField(help_text='赢票数量', default=0)

    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("miner_no",)}
        ]
    }


class MinerDay(MongoBaseModel):
    date = fields.StringField(help_text='时间', db_index=True)
    miner_no = fields.StringField(help_text='矿工no')
    actual_power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="原值算力", precision=0, default=0)
    sector_size = fields.LongField(help_text="sector_size")
    sector_all = fields.LongField(help_text="总扇区数")
    sector_effect = fields.LongField(help_text="有效扇区数")
    total_balance = Decimal2Field(help_text='余额', precision=0, default=0)
    available_balance = Decimal2Field(help_text='可用余额', precision=0, default=0)
    # pledge_balance = Decimal2Field(help_text='质押余额', precision=0, default=0)
    precommit_deposits_balance = Decimal2Field(help_text="预交付保证金", precision=0, default=0)
    initial_pledge_balance = Decimal2Field(help_text='扇区抵押额', precision=0, default=0)
    locked_balance = Decimal2Field(help_text='挖矿锁仓额', precision=0, default=0)
    total_reward = Decimal2Field(help_text='累计出块奖励', precision=0, default=0)
    total_block_count = fields.LongField(help_text='累计出块数量', default=0)
    total_win_count = fields.LongField(help_text='累计赢票数量', default=0)
    increase_power = Decimal2Field(help_text='新增算力', precision=0, default=0)
    increase_power_offset = Decimal2Field(help_text='新增算力差值', precision=0, default=0)
    avg_reward = Decimal2Field(help_text='平均挖矿收益（产出效率）atto FIL/TiB', precision=0, default=0)
    lucky = Decimal2Field(help_text='幸运值', precision=4, default=0)
    block_reward = Decimal2Field(help_text='出块奖励', precision=0, default=0)
    block_count = fields.IntField(help_text='出块数量', default=0)
    win_count = fields.IntField(help_text='赢票数量', default=0)
    pre_gas = Decimal2Field(help_text='pre_gas费', precision=0, default=0)
    pre_gas_count = fields.LongField(help_text='pre_gas次数', default=0)
    prove_gas = Decimal2Field(help_text='prove_gas', precision=0, default=0)
    prove_gas_count = fields.LongField(help_text='prove_gas次数', default=0)
    win_post_gas = Decimal2Field(help_text='win_post_gas费', precision=0, default=0)
    win_post_gas_count = fields.LongField(help_text='win_post_gas', default=0)
    pledge_gas = Decimal2Field(help_text='质押gas', precision=0, default=0)
    overtime_pledge_fee = Decimal2Field('过期质押', precision=0, default=0)
    worker_balance = Decimal2Field(help_text='worker余额', precision=0, default=0)
    post_balance = Decimal2Field(help_text='post余额', precision=0, default=0)
    owner_balance = Decimal2Field(help_text='owner余额', precision=0, default=0)

    meta = {
        "db_alias": "business",
        "strict": False,
        "index_background": True,
        'indexes': [
            {"fields": ("miner_no", "date"), "unique": True},
            {"fields": ("-actual_power",)},
            {"fields": ("-date",)}
        ]
    }


class MinerSyncLog(MongoBaseModel):
    '''同步日志'''
    date = fields.StringField(help_text='时间', db_index=True)
    gas_sync_height = fields.LongField(help_text='gas费同步到的高度', default=0)
    meta = {
        "db_alias": "business",
        "index_background": True
    }


class MinerDealPriceHistory(MongoBaseModel):
    """矿工订单出价数据热表，只保留48小时数据"""
    miner_no = fields.StringField(help_text='矿工no', db_index=True)
    record_time = fields.DateTimeField(help_text='记录时间')
    max_piece_size = Decimal2Field(help_text='最大快大小', precision=0, default=0)
    min_piece_size = Decimal2Field(help_text='最小快大小', precision=0, default=0)
    price = Decimal2Field(help_text='价格', default=0, precision=0)
    verified_price = Decimal2Field(help_text='验证价格', default=0, precision=0)

    meta = {
        "db_alias": "business",
        'indexes': [
            {"fields": ("miner_no", "record_time")}
        ]
    }


class MinersPool(MongoBaseModel):
    """
    存储池
    """
    owner_id = fields.StringField()
    owner_address = fields.StringField()
    miner_list = fields.ListField()
    total_balance = Decimal2Field(help_text="余额", precision=0, default=0)
    available_balance = Decimal2Field(help_text="可用余额", precision=0, default=0)
    # pledge_balance = fields.FloatField(help_text="质押余额")
    initial_pledge_balance = Decimal2Field(help_text="扇区抵押额", precision=0, default=0)
    locked_balance = Decimal2Field(help_text="锁仓额", precision=0, default=0)
    actual_power = Decimal2Field(help_text="有效算力", precision=0, default=0)
    raw_power = Decimal2Field(help_text="原值算力", precision=0, default=0)
    sector_all = fields.LongField(help_text="总扇区数")
    sector_effect = fields.LongField(help_text="有效扇区数")
    sector_faults = fields.LongField(help_text="无效扇区数")
    sector_recovering = fields.LongField(help_text="恢复中扇区数")
    total_reward = Decimal2Field(help_text="累计奖励", precision=0, default=0)
    total_block_count = fields.LongField(help_text="累计奖励", precision=0, default=0)
    total_win_count = fields.LongField(help_text="累计赢票数量", precision=0, default=0)

    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("owner_id",)},
            {"fields": ("owner_address",)},
            {"fields": ("actual_power",)},
        ]
    }
