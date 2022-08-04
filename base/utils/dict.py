import enum
from datetime import datetime, date


def dict_diff(d1, d2):
    adds, dels, changes, remains = set(), set(), set(), set()
    for k in list(d1.keys()) + list(d2.keys()):
        if k not in d1:  # 新增
            adds.add(k)
        elif k not in d2:  # 删除
            dels.add(k)
        elif d1[k] != d2[k]:  # 更改
            changes.add(k)
        else:  # 不变
            remains.add(k)
    return adds, dels, changes, remains


def dict_deep_update(d1, d2):
    """
    字典深度合并
    :param d1:
    :param d2:
    :return:
    """
    for key, value in d2.items():
        if key not in d1 or not (isinstance(d1[key], dict) and isinstance(value, dict)):
            d1[key] = value
        else:
            d1[key] = dict_deep_update(d1[key], value)
    return d1


def result_to_dict(result, only_fields=[], exclude_fields=[], bool_fields=[]):
    """
    Result转化为dict
    :param result: 结果对象
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
            val = val.strftime('%Y-%m-%d %H:%M')
        if isinstance(val, date):
            val = val.strftime('%Y-%m-%d')
        if isinstance(val, enum.Enum):
            val = val.value
        return val

    tmp = dict()
    # 只获取的值
    for field in only_fields:
        value = get_val(getattr(result, field, None))
        if field in bool_fields:
            value = bool(value)
        tmp[field] = value
    if tmp:
        return tmp

    # 正常处理
    fields = []
    if hasattr(result, '_sa_instance_state') and hasattr(result._sa_instance_state, 'attrs'):
        fields = [c.key for c in result._sa_instance_state.attrs]
    elif hasattr(result, '_fields'):
        fields = result._fields

    for field in fields:
        if field in exclude_fields:
            continue
        value = get_val(getattr(result, field, None))
        if field in bool_fields:
            value = bool(value)
        tmp[field] = value

    return tmp
