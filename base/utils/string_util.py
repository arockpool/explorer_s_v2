import re

from datetime import date, datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from decimal import Decimal, ROUND_HALF_UP


def is_mobile(val):
    """
    匹配字符串是否是手机
    :param val:
    :return:
    """
    return re.match(r"^1[3456789]\d{9}$", str(val)) is not None


def is_email(val):
    """
    匹配字符串是否是邮箱
    :param val:
    :return:
    """
    return re.match(r"^[-_\w\.]{0,64}@([-\w]{1,63}\.)*[-\w]{1,63}$", str(val)) is not None


def trip_number(value):
    if value.endswith('.0'):
        value = value[0:-2]
    elif re.search("^-?\d+(\.\d+)?(e[-+]\d+)$", value):
        value = str(int(float(value)))
    return value


def trip_bool(value):
    if value.strip() == '是':
        return True
    return False


def try_decimal(t):
    try:
        return Decimal(t)
    except:
        return 0


def try_float(t):
    try:
        return float(t)
    except:
        return 0


def is_float(t):
    try:
        float(t)
        return True
    except:
        return False


def round_half_up(v, digits=2):
    return Decimal(v).quantize(Decimal(f".{'0' * digits}"), ROUND_HALF_UP)


def bool_to_str(val):
    """
    bool类似转化为是或否，为空则保留
    """
    return val if not isinstance(val, bool) else val and '是' or '否'


def parse_date(value, pstr=True, format=None):
    """
    解析日期文本
    :param value:
    :param pstr: 是否输出为字符串
    :param format: 输出格式
    :return:
    """
    if not value:
        return None
    try:
        value = parse(value).date()
        if format:
            value = value.strftime(format)
        if pstr:
            value = str(value)
    except ValueError:
        value = None
    except TypeError:
        value = None
    except OverflowError:
        value = None
    return value


def try_parse_date(value):
    """处理2019-12-11 00:0000为2019-12-11，其余则保留原值"""
    reg = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}) 00')
    res_reg = reg.search(value)
    if value and res_reg:
        value = res_reg.group(1)
    return value


def calculate_age(birthday):
    born = parse(birthday).date()
    today = date.today()
    try:
        curr_born = born.replace(year=today.year)
    except ValueError:  # raised when birth date is February 29 and the current year is not a leap year
        curr_born = born.replace(year=today.year, day=born.day - 1)
    return today.year - born.year - (curr_born > today)


def get_between_months(start, end):
    """
    获取区间内所有月份
    :param start: 201903
    :param end: 201905
    :return: [201903, 201904, 201905]
    """
    fmt = '%Y%m'
    start_d = datetime.strptime(start, fmt)
    end_d = datetime.strptime(end, fmt)
    delta_month = abs((end_d.year - start_d.year) * 12 + (end_d.month - start_d.month) * 1)
    months = [start_d.strftime(fmt)]
    for n in range(delta_month):
        n += 1
        next_m = start_d + relativedelta(months=n)
        months.append(next_m.strftime(fmt))
    return months
