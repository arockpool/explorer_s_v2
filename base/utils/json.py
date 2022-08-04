import json
import re


def parse_value(value, only_fields=[]):
    try:
        if isinstance(value, list):
            value = [parse_value(d, only_fields) for d in v]
        elif isinstance(value, dict):
            value = values_to_json(value, only_fields)
        elif re.search("^\d+$", value):
            value = int(value)
        elif re.search("^\d+\.\d+$", value):
            value = float(value)
        elif value in ['true', 'false']:
            value = value == 'true' and True or False
        elif re.search("^[\[\{]{1}.*[\}\]]{1}$", value):
            value = values_to_json(json.loads(value), only_fields)
    except:
        pass
    return value


def values_to_json(data, only_fields=[]):
    """
    dict 数据转换，通常用户request.args数据转换
    :param data:
    :param only_fields:
    :return:
    """
    only_fields += ['page', 'pagesize']

    # 默认为dict
    temp = {}
    for field, value in (data or {}).items():
        if field in only_fields:
            temp[field] = parse_value(value, only_fields)
        else:
            temp[field] = value
    return temp


if __name__ == '__main__':
    data = {
        'keywords': '1',
        'extra_fields': {
            'page': '10',
            'active': 'true',
            'enable': 'false'
        },
        'active': 'false',
        'page': '1',
        'pagesize': 10
    }
    print(values_to_json(data, only_fields=['active', 'extra_fields']))
