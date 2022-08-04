import decimal
from decimal import Decimal
from mongoengine import fields

class CustomDictField(fields.DictField):
    def replace_value(self, value, save=True):
        if isinstance(value, dict):
            for k, v in list(value.items()):
                old_key = k
                if save:
                    k = k.replace(".", "{{dot}}").replace("$", "{{dollar}}")
                else:
                    k = k.replace("{{dot}}", ".").replace("{{dollar}}", "$")
                new_key = k
                value[new_key] = self.replace_value(value.pop(old_key), save=save)
        if not save:
            if isinstance(value, float):
                return Decimal(value)
        else:
            if isinstance(value, Decimal):
                return float(value)
        return value

    def validate(self, value):
        value = self.replace_value(value, save=True)
        super(CustomDictField, self).validate(value)

    def to_python(self, value):
        value = super().to_python(value)
        return self.replace_value(value, save=False)

    def to_mongo(self, value, use_db_field=True, fields=None):
        value = super().to_mongo(value, use_db_field, fields)
        return self.replace_value(value, save=True)


class ReUsableObject():

    def __init__(self, default=None, obj_type=None):
        self.data = None
        self.default = default
        self.obj_type = obj_type

    def __getattr__(self, item):
        t = self.data.get(item, self.default)
        if self.obj_type:
            return self.obj_type(t)
        else:
            return t

    def __call__(self, data):
        self.data = data
        return self


def cvt_dict_decimal(d: dict, to_type=str):
    """
    转换字典中的decimal
    不检测循环
    :param d:
    :param to_type:
    :return:
    """
    for k, v in d.items():
        if isinstance(v, decimal.Decimal):
            d[k] = to_type(v)
        elif isinstance(v, dict):
            cvt_dict_decimal(v, to_type)
        elif isinstance(v, (tuple, list)):
            d[k] = type(v)(cvt_dict_decimal(iv, to_type) for iv in v)
    return d
