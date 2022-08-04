from datetime import datetime, date
import decimal
from bson.decimal128 import Decimal128
from mongoengine import connect, Document
from mongoengine.fields import DecimalField
from base.utils.fil import utc2local


class MongoBaseModel(Document):
    meta = {
        'abstract': True
    }

    def to_dict(self, only_fields=[], exclude_fields=[], bool_fields=[]):
        """
        Modle转化为dict
        :param only_fields:  有值时，只返回指定字段的值 exclude_fields 失效
        :param exclude_fields: 有值时, 返回值排除
        :param bool_fields: 有值时, 将值转成bool类型
        :return:
        """

        def get_val(val):
            """
            类型识别
            :param val:
            :return:
            """
            if isinstance(val, datetime):
                val = utc2local(val).strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(val, date):
                val = val.strftime('%Y-%m-%d')
            return val

        tmp = dict()
        # 只获取的值
        for field in only_fields:
            value = get_val(getattr(self, field, None))
            if field in bool_fields:
                value = bool(value)
            tmp[field] = value
        if tmp:
            return tmp

        # 正常处理
        for name in self._data:
            if name in exclude_fields:
                continue
            value = get_val(getattr(self, name, None))
            if name in bool_fields:
                value = bool(value)
            # if isinstance(value, (float, decimal.Decimal)):
            #     value = str(value)
            tmp[name] = value
        # 特殊处理 当有字段是id时
        if self._data.get("auto_id_0"):
            tmp["id"] = str(self._data.get("auto_id_0"))
            tmp.pop("_id", None)
            tmp.pop("auto_id_0", None)
        else:
            tmp["id"] = str(tmp["id"])
        return tmp


class Decimal2Field(DecimalField):
    def to_python(self, value):
        if value is None:
            return value

        # Convert to string for python 2.6 before casting to Decimal
        try:
            value = decimal.Decimal("%s" % value)
            if self.precision:
                return value.quantize(
                    decimal.Decimal(".%s" % ("0" * self.precision)), rounding=self.rounding
                )
            return value.quantize(decimal.Decimal("1"), rounding=self.rounding)
        except (TypeError, ValueError, decimal.InvalidOperation):
            return value
        # if self.precision:
        #     return value.quantize(
        #         decimal.Decimal(".%s" % ("0" * self.precision)), rounding=self.rounding
        #     )
        # print (self.db_field)
        # print (value)
        # return value.quantize(decimal.Decimal("1"), rounding=self.rounding)

    def to_mongo(self, value):
        if value is None:
            return value
        if self.force_string:
            return str(self.to_python(value))
        return Decimal128(self.to_python(value))


def init_mongodb(config):
    connect(
        db=config.get('MONGODB_NAME'),
        alias="business",
        username=config.get('MONGODB_USER', None),
        password=config.get('MONGODB_PASSWORD', None),
        host=config.get('MONGODB_HOST'),
        authentication_source='admin'
    )
    connect(db="explorer_base_v2", alias="base",
            username=config.get('MONGODB_USER', None),
            password=config.get('MONGODB_PASSWORD', None),
            host=config.get('MONGODB_HOST'),
            authentication_source='admin'
            )
