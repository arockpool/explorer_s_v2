import datetime


def trends_parm(search_type, start_date=None, end_date=None, spot=48):
    """
    趋势图数据预先处理
    :param search_type:
    :param start_date:
    :param end_date:
    :param spot:
    :return:
    """
    if start_date and end_date:
        start_date = datetime.datetime.strptime(start_date+" 00:00:00", '%Y-%m-%d %H:%M:%S')
        end_date = datetime.datetime.strptime(end_date+" 23:59:59", '%Y-%m-%d %H:%M:%S')
        days = (end_date-start_date).days or 1
    else:
        search_type_dict = {"day": 1, "week": 7, "month": 30, "season": 90, "half_year": 180, "year": 365}
        days = search_type_dict[search_type]
        now = datetime.datetime.now()
        start_date = (now - datetime.timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        end_date = now.strftime('%Y-%m-%d %H:%M:%S')
    step = int(2880 * days / spot)
    return step, start_date, end_date
