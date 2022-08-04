from base.db.mongo import MongoBaseModel, Decimal2Field
import mongoengine.fields as fields


class Messages(MongoBaseModel):
    msg_cid = fields.StringField()
    msg_to = fields.StringField()
    msg_from = fields.StringField()
    msg_value = Decimal2Field(help_text="值", precision=0, default=0)
    msg_method = fields.IntField()
    msg_method_name = fields.StringField()
    sector_count = fields.IntField(help_text="扇区数", default=0)
    sector_nums = fields.ListField(help_text="扇区", default=[])
    gascost_total_cost = Decimal2Field(help_text="消息总GAS", precision=0, default=0)
    gascost_miner_penalty = Decimal2Field(help_text="惩罚", precision=0, default=0)
    msgrct_exit_code = fields.IntField(help_text="消息返回结果")
    base_fee = fields.LongField()
    height = fields.LongField()
    height_time = fields.DateTimeField()
    sector_size_value = fields.LongField()
    block_hash_arr = fields.ListField(help_text="区块ID", default=[])
    meta = {
        "db_alias": "base",
        "strict": False,
        "index_background": False,
        'indexes': [
            {"fields": ("msg_cid",)},
            {"fields": ("-height",)},
            {"fields": ("msg_method_name", "-height")},
            {"fields": ("msgrct_exit_code", "-height")},
            {"fields": ("msg_to", "msg_method_name", "-height")},
            {"fields": ("msg_from", "msg_method_name", "-height")},
            {"fields": ("block_hash_arr", "msg_method_name", "-height")},
        ]
    }


class MessagesStat(MongoBaseModel):
    """
    消息查询数据预处理
    """
    table_name = fields.StringField()
    msg_method_name = fields.StringField(help_text="消息方法")
    msgrct_exit_code = fields.IntField(help_text="状态")
    count = fields.LongField(help_text="数量")
    meta = {
        "db_alias": "business",
        "strict": False,
        "index_background": False,
        'indexes': [
            {"fields": ("table_name", "msg_method_name",)},
            {"fields": ("table_name", "msgrct_exit_code",)},
            {"fields": ("table_name",  "msg_method_name", "msgrct_exit_code")},
        ]
    }


class MessagesStatLog(MongoBaseModel):
    """
    消息查询数据预处理 高度记录
    """
    height = fields.LongField()
    meta = {
        "db_alias": "business",
        "strict": False
    }


class MessagesStatInfo(MongoBaseModel):
    """
    消息知道对象的查询数据预处理
    """
    msg_key = fields.StringField()
    table_name = fields.StringField()
    count = fields.LongField(help_text="数量")
    meta = {
        "db_alias": "business",
        "strict": False,
        'indexes': [
            {"fields": ("msg_key", "-table_name")},
        ]
    }


class MessagesAll(MongoBaseModel):
    msg_cid = fields.StringField()
    msg_to = fields.StringField()
    msg_from = fields.StringField()
    msg_nonce = fields.IntField(help_text="随机数")
    msg_value = Decimal2Field(precision=0, default=0)
    msg_gas_limit = fields.IntField()
    msg_gas_fee_cap = fields.IntField()
    msg_gas_premium = fields.IntField()
    msg_method = fields.IntField()
    msg_method_name = fields.StringField()
    msg_params = fields.DictField()
    msg_return = fields.DictField()
    gascost_gas_used = Decimal2Field(precision=0, default=0)
    gascost_base_fee_burn = Decimal2Field(help_text="基础燃烧", precision=0, default=0)
    gascost_over_estimation_burn = Decimal2Field(help_text="超额燃烧", precision=0, default=0)
    gascost_miner_penalty = Decimal2Field(help_text="惩罚", precision=0, default=0)
    gascost_miner_tip = Decimal2Field(help_text="矿工小费", precision=0, default=0)
    gascost_refund = Decimal2Field(help_text="退款", precision=0, default=0)
    gascost_total_cost = Decimal2Field(help_text="消息总GAS", precision=0, default=0)
    msgrct_exit_code = fields.IntField(help_text="消息返回结果")
    base_fee = fields.LongField()
    height = fields.LongField()
    height_time = fields.DateTimeField()
    sector_size = fields.StringField()
    sector_size_value = fields.LongField()
    type = fields.StringField()
    synced_at_str = fields.DateTimeField()
    synced_at = fields.IntField()
    meta = {
        "db_alias": "base",
        "strict": False,
        "index_background": False,
        'indexes': [
            {"fields": ("msg_cid",)},
            {"fields": ("-height",)},
            {"fields": ("msg_method_name", "-height")},
        ]
    }


class MigrateLog(MongoBaseModel):
    version = fields.StringField(unique=True)
    miner_day = fields.DictField(help_text="记录的内容")
    deal_content = fields.DictField(help_text="记录的内容")
    meta = {
        "db_alias": "base",
        "index_background": True
    }


class TipsetGas(MongoBaseModel):
    """
    每个高度的gas费汇总，统计base_gas、pre_gas、prov_gas、win_post_gas，用于计算生成成本、维护成本
    """
    height = fields.IntField()
    height_time = fields.DateTimeField()
    pre_gas_32 = Decimal2Field(help_tex='32pre_gas费', precision=0, default=0)
    pre_gas_count_32 = fields.IntField(help_tex='32pre_gas次数', default=0)
    prove_gas_32 = Decimal2Field(help_tex='32prove_gas', precision=0, default=0)
    prove_gas_count_32 = fields.IntField(help_tex='32prove_gas次数', default=0)
    win_post_gas_32 = Decimal2Field(help_tex='32win_post_gas费', precision=0, default=0)
    win_post_gas_count_32 = fields.IntField(help_tex='32win_post_gas次数', default=0)
    pre_gas_64 = Decimal2Field(help_tex='64pre_gas费', precision=0, default=0)
    pre_gas_count_64 = fields.IntField(help_tex='64pre_gas次数', default=0)
    prove_gas_64 = Decimal2Field(help_tex='64prove_gas', precision=0, default=0)
    prove_gas_count_64 = fields.IntField(help_tex='64prove_gas次数', default=0)
    win_post_gas_64 = Decimal2Field(help_tex='64win_post_gas费', precision=0, default=0)
    win_post_gas_count_64 = fields.IntField(help_tex='64win_post_gas次数', default=0)
    base_fee = fields.LongField(help_tex='base_fee', default=0)
    create_gas_32 = Decimal2Field(help_tex='32G生产gas', precision=0, default=0)
    keep_gas_32 = Decimal2Field(help_tex='32G维护gas', precision=0, default=0)
    create_gas_64 = Decimal2Field(help_tex='64G生产gas', precision=0, default=0)
    keep_gas_64 = Decimal2Field(help_tex='64G维护gas', precision=0, default=0)

    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("-height",)},
        ]
    }


class TipsetGasStat(MongoBaseModel):
    """
   24小时内每个tipset的各种gas费汇总
    """
    height = fields.IntField()
    height_time = fields.DateTimeField()
    sector_type = fields.IntField(help_tex="扇区类型", choices=[0, 1], default=0)  # 0: 32G 1: 64G
    method = fields.StringField(help_tex='gas方法')
    count = fields.IntField(help_tex='次数', default=0)
    gas_limit = Decimal2Field(help_tex='gas_limit汇总', precision=0, default=0)
    gas_fee_cap = Decimal2Field(help_tex='gas_fee_cap汇总', precision=0, default=0)
    gas_premium = Decimal2Field(help_tex='gas_premium汇总', precision=0, default=0)
    gas_used = Decimal2Field(help_tex='gas_used汇总', precision=0, default=0)
    base_fee_burn = Decimal2Field(help_tex='base_fee_burn汇总', precision=0, default=0)
    over_estimation_burn = Decimal2Field(help_tex='over_estimation_burn汇总', precision=0, default=0)
    total_cost = Decimal2Field(help_tex='total_cost汇总', precision=0, default=0)
    msg_value = Decimal2Field(help_tex='消息金额', precision=0, default=0)
    meta = {
        "db_alias": "business",
        "index_background": True,
        'indexes': [
            {"fields": ("-height", "method", "sector_type")},
        ]
    }


class Mpool(MongoBaseModel):
    """消息池子"""
    cid = fields.StringField()
    msg_to = fields.StringField()
    msg_from = fields.StringField()
    from_nonce = fields.StringField()
    gas_limit = fields.IntField()
    gas_fee_cap = Decimal2Field(precision=0, default=0)
    gas_premium = Decimal2Field(precision=0, default=0)
    nonce = fields.IntField()
    method = fields.IntField()
    method_name = fields.StringField()
    height = fields.LongField()
    height_time = fields.DateTimeField()
    value = Decimal2Field(precision=0, default=0)
    version = fields.IntField()
    meta = {
        "db_alias": "base",
        "strict": False,
        'indexes': [
            {"fields": ("-height",)},
            {"fields": ("method_name", "msg_to", "msg_from", "-height")},
            {"fields": ("msg_from", "-height")},
            {"fields": ("msg_to", "-height")}
        ]
    }
