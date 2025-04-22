import time
import re
from datetime import datetime, timedelta


def date_to_timestamp(date, time_format="%Y-%m-%d %H:%M:%S"):
    """
    "2011-09-28 10:00:00"时间格式转化为时间戳
    :param date:
    :param time_format:
    :return:
    """
    timestamp = time.mktime(time.strptime(date, time_format))
    return int(timestamp)


def timestamp_to_date(timestamp, time_format="%Y-%m-%d %H:%M:%S"):
    """
    将时间戳转化为自定义格式
    :param timestamp:
    :param time_format:
    :return:
    """
    if timestamp is None:
        raise ValueError("timestamp is null")

    date = time.localtime(timestamp)
    return time.strftime(time_format, date)


def get_current_timestamp():
    """
    返回当前时间戳
    :return:
    """
    return int(time.time())


def get_current_date(date_format="%Y-%m-%d %H:%M:%S"):
    """
    返回当前时间
    :param date_format: 自定义格式
    :return:
    """
    return datetime.now().strftime(date_format)


def format_date(date, old_format="%Y-%m-%d %H:%M:%S", new_format="%Y-%m-%d %H:%M:%S", offest=0):
    """
    格式化时间到自定义格式
    :param date:
    :param old_format:
        %y 两位数的年份表示（00-99）
        %Y 四位数的年份表示（000-9999）
        %m 月份（01-12）
        %d 月内中的一天（0-31）
        %H 24小时制小时数（0-23）
        %I 12小时制小时数（01-12）
        %M 分钟数（00-59）
        %S 秒（00-59)
    :param new_format:
    :param offest: 时间偏移量(小时) 0:utc时间
    :return:
    """

    try:
        date_obj = datetime.strptime(date, old_format)
        if "T" in date and "Z" in date:
            date_obj += timedelta(hours=offest)
            date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            date_str = datetime.strftime(date_obj, new_format)

    except ValueError:
        print("日期格式化出错，old_format = %s 不符合 %s 格式" % (old_format, date))
        date_str = date

    return date_str


def format_time(release_time, date_format="%Y-%m-%d %H:%M:%S"):
    """
     format_time("2月前")
    '2021-08-15 16:24:36'
    :param release_time: 
    :param date_format: 
    :return: 
    """

    if "年前" in release_time:
        years = re.compile("(\d+)\s*年前").findall(release_time)
        years_ago = datetime.now() - timedelta(
            days=int(years[0]) * 365
        )
        release_time = years_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "月前" in release_time:
        months = re.compile("(\d+)[\s个]*月前").findall(release_time)
        months_ago = datetime.now() - timedelta(
            days=int(months[0]) * 30
        )
        release_time = months_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "周前" in release_time:
        weeks = re.compile("(\d+)\s*周前").findall(release_time)
        weeks_ago = datetime.now() - timedelta(days=int(weeks[0]) * 7)
        release_time = weeks_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "天前" in release_time:
        ndays = re.compile("(\d+)\s*天前").findall(release_time)
        days_ago = datetime.now() - timedelta(days=int(ndays[0]))
        release_time = days_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "小时前" in release_time:
        nhours = re.compile("(\d+)\s*小时前").findall(release_time)
        hours_ago = datetime.now() - timedelta(hours=int(nhours[0]))
        release_time = hours_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "分钟前" in release_time:
        nminutes = re.compile("(\d+)\s*分钟前").findall(release_time)
        minutes_ago = datetime.now() - timedelta(
            minutes=int(nminutes[0])
        )
        release_time = minutes_ago.strftime("%Y-%m-%d %H:%M:%S")

    elif "今天" in release_time:
        release_time = release_time.replace("今天", get_current_date("%Y-%m-%d %H:%M:%S"))

    elif "刚刚" in release_time:
        release_time = get_current_date()

    template = re.compile("(\d{4}-\d{1,2}-\d{2})(\d{1,2})")
    release_time = re.sub(template, r"\1 \2", release_time)
    release_time = format_date(release_time, new_format=date_format)

    return release_time