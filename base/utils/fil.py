import re
import json
import time
import random
import datetime
import decimal
from bson.decimal128 import Decimal128


def format_price(price, point=2, round_flag=decimal.ROUND_HALF_EVEN):
    '''
    格式化价格
    round_flag:小数保留标志
    '''
    return str(decimal.Decimal(price).quantize(decimal.Decimal("1.{}".format('0' * point)),
                                               round_flag) if price is not None else '')
    # return ('%.' + str(point) + 'f') % decimal.Decimal(price).quantize(decimal.Decimal()) if price is not None else ''


def format_fil(num, point=4, round_flag=decimal.ROUND_HALF_EVEN):
    '''
    格式化价格
    '''
    num = decimal.Decimal(num) / 10 ** 18
    return format_price(price=num, point=point, round_flag=round_flag)


def format_fil_to_decimal(num, round_data=None):
    '''
    格式化价格
    '''

    num = decimal.Decimal(num) / 10 ** 18
    if round_data:
        return round(num, round_data)
    return num


def format_fil_to_decimal_verification(num):
    '''
    格式化,需要进行验证操作
    仅转换乘过10^18的数据
    '''
    try:
        num = decimal.Decimal(num)
    except:
        return num
    else:
        if num > 10 ** 10 or num < -10 ** 10:
            num = num / 10 ** 18
        else:
            return num
    finally:
        return num


def un_format_fil_to_decimal(num, point=4):
    '''
    反格式化价格,乘10^18
    '''
    num = decimal.Decimal(num) * 1000000000000000000
    return num


def format_result_dict(result_dict):
    """
    将参数进行格式化操作(除10^18)
    """
    return dict(zip(result_dict, map(format_fil_to_decimal_verification, result_dict.values())))


def format_result_dict_auto_unit(result_dict):
    """
    将参数进行格式化操作(除10^18)
    """
    return dict(zip(result_dict, map(format_coin_to_str, result_dict.values())))


def clear_html_tag(text):
    '''
    去除掉所有的html标签
    '''
    from html.parser import HTMLParser
    html_parser = HTMLParser()
    temp_text = html_parser.unescape(text)

    rule = re.compile(r'<[^>]+>', re.S)
    return rule.sub('', temp_text) if text else text


def generate_random_num(length=4):
    '''
    随机号码
    '''
    return ''.join([random.choice('123456789ABCDEFGHJKLMNPQRSTUVWXYZ')
                    for x in range(length)])


def generate_tx_code():
    return datetime.datetime.now().replace(microsecond=0).strftime(
        "%Y%m%d%H%M%S") + (generate_random_num(length=18))


def format_power(power, unit='DiB'):
    '''
    格式化power
    '''
    units = ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB", "BiB", "NiB", "DiB"]
    unit_index = 0

    power = decimal.Decimal(power)
    _power = abs(power)
    temp = _power
    while (temp >= 1024) and (unit_index <= len(units)):
        unit_index += 1
        temp = temp / 1024
        if units[unit_index] == unit:
            break

    # 正负判断
    return '%s%s %s' % ('-' if power < 0 else '', format_price(temp), units[unit_index])


def format_power_to_TiB(power):
    '''
    格式化power
    '''
    units = ["Bytes", "KiB", "MiB", "GiB", "TiB"]
    unit_index = 0

    power = decimal.Decimal(power)
    temp = power
    while (abs(temp) > 1024) and (unit_index < len(units) - 1):
        unit_index += 1
        temp = temp / 1024

    return '%s %s' % (format_price(temp), units[unit_index])


def str_2_power(power_str):
    '''
    根据算力字符串还原成byte
    '''
    if power_str == "" or power_str is None:
        return power_str
    mapping = {
        'NiB': 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
        'YiB': 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
        'ZiB': 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
        'EiB': 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
        'PiB': 1024 * 1024 * 1024 * 1024 * 1024,
        'TiB': 1024 * 1024 * 1024 * 1024,
        'GiB': 1024 * 1024 * 1024,
        'MiB': 1024 * 1024,
        'KiB': 1024,
        'Bytes': 1
    }

    value, unit = power_str.split(' ')
    return int(float(value) * mapping.get(unit, 1))


# def prove_commit_aggregate_gas(sector_numbers, base_fee):
#     """
#     ProveCommitAggregate除了按之前方法取gas外，还需要按下述描述，计算聚合费用
#     """
#     batch_discount = 1 / 20  # unitless批量证明折扣，固定
#     batch_balancer = 2 * (10 ** 9)  # attoFIL 批量证明最低限额，固定
#     single_proof_gas_usage = 65733297  # 单扇区证明gas费率，固定
#
#     bath_gas_fee = max(batch_balancer, base_fee)  # 批量gasFee = 批量证明最低限额与消息对应高度baseFee的最大值
#     bath_gas_charge = bath_gas_fee * single_proof_gas_usage * sector_numbers * batch_discount
#     return bath_gas_charge


def get_aggregate_gas(sector_numbers, base_fee, height=1231620, msg_method=26):

    """
    ProveCommitAggregate除了按之前方法取gas外，还需要按下述描述，计算聚合费用
    lotus v1.10.0仅对ProveCommitAggregate收取消息聚合费
    lotus v1.12.0对PreCommitSectorBatch和ProveCommitAggregate都收取聚合费
    """
    if msg_method not in [25, 26]:
        return 0
    batch_discount = 1 / 20  # unitless批量证明折扣，固定
    if height >= 1231620:
        batch_balancer = 5 * (10 ** 9)  # attoFIL 批量证明最低限额，固定
        if msg_method == 25:
            single_proof_gas_usage = 16433324.1825  # SinglePreCommitGasUsage = 16433324.1825   单扇区证明gas费率，固定
        if msg_method == 26:
            single_proof_gas_usage = 49299972.5475  # SingleProveCommitGasUsage = 49299972.5475 单扇区证明gas费率，固定
    else:
        batch_balancer = 2 * (10 ** 9)  # attoFIL 批量证明最低限额，固定
        if msg_method == 25:
            single_proof_gas_usage = 0
        if msg_method == 26:
            single_proof_gas_usage = 65733297  # 单扇区证明gas费率，固定

    bath_gas_fee = max(batch_balancer, base_fee)  # 批量gasFee = 批量证明最低限额与消息对应高度baseFee的最大值
    bath_gas_charge = bath_gas_fee * single_proof_gas_usage * sector_numbers * batch_discount
    return bath_gas_charge


def format_float_coin(coin, point=3, flag=decimal.Decimal(10 ** -5)):
    """
    格式化coin的值,删除末尾的0,前进单位
    """
    # units = ["femto", "pico", "nano", "micro", "milli", ""]
    units = ["atto", "nano", ""]
    unit_index = 0

    power = decimal.Decimal(coin)
    temp = power / 10 ** 9
    while not (temp < flag) and (unit_index < len(units) - 1):
        unit_index += 1
        temp = temp / 10 ** 9
    temp *= 10 ** 9
    return '%s %s' % (format_price(temp, point=point), units[unit_index])


def format_coin_to_str(power, temp_value=10 ** 9, abandon=False, carry=6):
    '''
    temp:判断需要小于多少
    格式化power
    carry:向前进位参数   10**carry次方后再向前进位
    '''
    # units = ["femto", "pico", "nano", "micro", "milli", ""]
    units = ["atto", "nano", ""]
    unit_index = 0

    power = abs(decimal.Decimal(power))
    if power < 10 ** carry and abandon:
        return 0
    if power < 10 ** carry:
        return '%s %s' % (format_price(power), units[unit_index])
    temp = power / 10 ** 9
    unit_index += 1
    while (temp >= temp_value) and (unit_index < len(units) - 1):
        temp = temp / 10 ** 9
        unit_index += 1

    return '%s %s %s' % ('-' if power < 0 else "", format_price(temp, 4), units[unit_index])


def _d(v):
    return decimal.Decimal(v)


def height_to_datetime(height, need_format=False):
    '''高度转换成时间'''
    launch_date = datetime.datetime(2020, 8, 25, 6, 0, 0)
    seconds = int(height) * 30
    d = launch_date + datetime.timedelta(seconds=seconds)
    return d.strftime('%Y-%m-%d %H:%M:%S') if need_format else d


def datetime_to_height(d=None):
    '''时间转换成高度'''
    launch_date = datetime.datetime(2020, 8, 25, 6, 0, 0)
    if d is None:
        temp = datetime.datetime.now().replace(microsecond=0)
    else:
        if isinstance(d, str):
            temp = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        elif isinstance(d, datetime.datetime):
            temp = d
        elif isinstance(d, datetime.date):
            temp = datetime.datetime.combine(d, datetime.time.min)
        else:
            return 0
    return int((temp - launch_date).total_seconds() / 30)


def local2utc(local_dtm):
    """
    本地转UTC
    :param local_dtm:
    :return:
    """
    return datetime.datetime.utcfromtimestamp(local_dtm.timestamp())


def utc2local(utc_dtm):
    """
    UTC转本地
    :param utc_dtm:
    :return:
    """
    utc_tm = datetime.datetime.utcfromtimestamp(0)
    local_tm = datetime.datetime.fromtimestamp(0)
    offset = local_tm - utc_tm
    return utc_dtm + offset


def bson_to_decimal(decimal128, precision=0):
    """
    Mongo的数据转化为普通decimal
    :param decimal128:
    :param precision:
    :return:
    """
    if not decimal128:
        return _d(0)
    if not isinstance(decimal128, Decimal128):
        return _d(decimal128)
    value = decimal128.to_decimal()
    try:
        if precision:
            return value.quantize(
                decimal.Decimal(".%s" % ("0" * precision)), rounding=decimal.ROUND_HALF_UP
            )
        return value.quantize(decimal.Decimal("0"), rounding=decimal.ROUND_HALF_UP)
    except (TypeError, ValueError, decimal.InvalidOperation):
        return value


def bson_dict_to_decimal(data={}, precision=0):
    """
    Mongo的数据转化为普通decimal
    :param data:
    :param precision:
    :return:
    """
    result = {}
    for key, value in data.items():
        if isinstance(value, Decimal128):
            result[key] = bson_to_decimal(value)
        else:
            result[key] = value
    return result
