import datetime
import mongoengine.fields as fields
from base.db.mongo import MongoBaseModel, Decimal2Field


class Notaries(MongoBaseModel):
    """
    公证人
    """
    id_address = fields.StringField(help_text='ID')
    name = fields.StringField(help_text='名称')
    org = fields.StringField(help_text='组织')
    address = fields.StringField(help_text='地址')
    region = fields.StringField(help_text='区域')
    media = fields.StringField()
    use_case = fields.StringField()
    application_link = fields.StringField()
    github_account = fields.StringField()
    granted_allowance = Decimal2Field(help_text='总限额', precision=0, default=0)
    allocated_allowance = Decimal2Field(help_text='分派的限额', precision=0, default=0)
    client_count = fields.IntField(help_text='客户数')
    clients = fields.ListField(help_text='客户(去重复)')
    create_time = fields.DateTimeField(default=datetime.datetime.utcnow)
    meta = {
        "db_alias": "business",
        'indexes': [
            {"fields": ("id_address",)},
            {"fields": ("address",)},
        ]
    }


class PlusClient(MongoBaseModel):
    """
    plus客户
    """
    id_address = fields.StringField(help_text='ID')
    name = fields.StringField(help_text='名称')
    address = fields.StringField(help_text='地址')
    region = fields.StringField(help_text='区域')
    media = fields.StringField()
    notaries = fields.DictField(help_text='公证人信息')
    allocated_allowance = Decimal2Field(help_text='分派的限额', precision=0, default=0)
    use_allowance = Decimal2Field(help_text='已经使用的', precision=0, default=0)
    msg_cid = fields.StringField()
    comments_url = fields.StringField()
    assignor = fields.StringField()
    assignee = fields.StringField()
    deal_count = fields.IntField(help_text='订单数')
    provider_count = fields.IntField(help_text='存储供应商数量数')
    providers = fields.ListField(help_text='存储供应商(去重复)')
    create_time = fields.DateTimeField(default=datetime.datetime.utcnow)
    meta = {
        "db_alias": "business",
        "strict": False,
        'indexes': [
            {"fields": ("id_address", "-deal_count")},
            {"fields": ("address",)},
            {"fields": ("notaries.id_address",)},
            {"fields": ("notaries.address",)},
        ]
    }


class Provider(MongoBaseModel):
    """
    存储提供者
    """
    miner_no = fields.StringField(help_text='ID')
    deal_count = fields.IntField(help_text='订单数')
    client_count = fields.IntField(help_text='客户数')
    clients = fields.ListField(help_text='客户(去重复)')
    use_allowance = Decimal2Field(help_text='已经使用的', precision=0)
    storage_price_per_epoch = Decimal2Field(help_text="每高度每byte单价", precision=0, default=0)
    avg_price = Decimal2Field(help_text="平均每高度每byte单价", precision=0, default=0)
    create_time = fields.DateTimeField(default=datetime.datetime.utcnow)
    meta = {
        "db_alias": "business",
        'indexes': [
            {"fields": ("miner_no",)}
        ]
    }


class DatacapDay(MongoBaseModel):
    """
    datacap统计
    """
    date = fields.StringField()
    client_count = fields.IntField(help_text='客户数')
    provider_count = fields.IntField(help_text='存储提供商数')
    deal_count = fields.IntField(help_text='订单数')
    use_size = Decimal2Field(help_text='已经使用的', precision=0)
    deal_gas_by_t = Decimal2Field(help_text='单T订单成本', precision=0)

    meta = {
        "db_alias": "business",
        "strict": False,
        'indexes': [
            {"fields": ("date",)}
        ]
    }
