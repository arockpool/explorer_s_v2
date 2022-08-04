import mongoengine.fields as fields
from base.db.mongo import MongoBaseModel, Decimal2Field


class Deal(MongoBaseModel):
    deal_id = fields.IntField(help_text="订单id")
    piece_cid = fields.StringField(help_text="文件cid")
    piece_size = Decimal2Field(help_text="文件大小", precision=0, default=0)
    is_verified = fields.BooleanField(help_text="是否已验证")
    client = fields.StringField(help_text="客户")
    provider = fields.StringField(help_text="托管矿工")
    start_epoch = fields.IntField(help_text="存储开始高度")
    end_epoch = fields.IntField(help_text="存储结束高度")
    storage_price_per_epoch = Decimal2Field(help_text="每高度每byte单价", precision=0, default=0)
    provider_collateral = Decimal2Field(help_text="托管矿工抵押", precision=0, default=0)
    client_collateral = Decimal2Field(help_text="客户抵押", precision=0, default=0)
    msg_id = fields.StringField(help_text="消息msg_cid")
    height = fields.IntField()
    height_time = fields.DateTimeField()

    meta = {
        "db_alias": "base",
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
            {"fields": ("deal_id", "-height")},
            {"fields": ("client", "-height")},
            {"fields": ("provider", "-height")}
        ]
    }


class Dealinfo(MongoBaseModel):
    """
    存储池里面的订单
    """
    deal_id = fields.StringField(help_text="订单id")
    client = fields.StringField(help_text="客户")
    client_collateral = Decimal2Field(help_text="客户抵押", precision=0, default=0)
    end_epoch = fields.IntField(help_text="存储结束高度")
    label = fields.StringField(help_text="label")
    last_updated_epoch = fields.IntField(help_text="最后更新高度")
    piece_cid = fields.StringField(help_text="文件cid")
    piece_size = Decimal2Field(help_text="文件大小", precision=0, default=0)
    provider = fields.StringField(help_text="托管矿工")
    provider_collateral = Decimal2Field(help_text="托管矿工抵押", precision=0, default=0)
    sector_start_epoch = fields.IntField(help_text="接单高度")
    slash_epoch = fields.IntField(help_text="")
    start_epoch = fields.IntField(help_text="存储开始高度")
    storage_price_perepoch = Decimal2Field(help_text="每高度每byte单价", precision=0, default=0)
    verified_deal = fields.BooleanField(help_text="是否已验证")

    meta = {
        "db_alias": "base",
        "strict": False,
        'indexes': [
            {"fields": ("-sector_start_epoch",)},
            {"fields": ("deal_id", "-sector_start_epoch")},
            {"fields": ("client", "-sector_start_epoch")},
            {"fields": ("provider", "-sector_start_epoch")}
        ]
    }


class DealStat(MongoBaseModel):
    """
    订单生态统计（24h）
    """
    height = fields.LongField(help_text="订单count")
    deal_count = fields.LongField(help_text="订单count")
    data_size = Decimal2Field(help_text="文件大小", precision=0, default=0)
    verified_deal_count = fields.LongField(help_text="验证订单count")
    verified_data_size = Decimal2Field(help_text="验证订单文件大小", precision=0, default=0)
    client_count = fields.LongField(help_text="客户count")
    piece_cid_count = fields.LongField(help_text="文件cid_count(去重)")
    deal_gas_by_t = Decimal2Field(help_text='单T订单成本', precision=0)
    avg_price = Decimal2Field(help_text='平均价格', precision=0)

    meta = {
        "db_alias": "business",
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
        ]
    }


class DealDay(MongoBaseModel):
    """
    订单生态统计（days）
    """
    date = fields.StringField(help_text="日期")
    deal_count = fields.LongField(help_text="订单count")
    verified_deal_count = fields.LongField(help_text="验证订单count")
    data_size = Decimal2Field(help_text="文件大小", precision=0, default=0)
    verified_data_size = Decimal2Field(help_text="验证订单文件大小", precision=0, default=0)
    client_count = fields.LongField(help_text="客户count")
    deal_gas_by_t = Decimal2Field(help_text='单T订单成本', precision=0)

    meta = {
        "db_alias": "business",
        "strict": False,
        'indexes': [
            {"fields": ("-date",)},
        ]
    }