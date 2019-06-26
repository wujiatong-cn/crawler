# -*- coding: utf-8 -*-

from datetime import datetime, timezone


# 判空
def check_blank(val):
    if val is None or val == "" or val == " ":
        return True
    return False


# 字符串时间转换为UTC时间
def str_to_utc_datetime(val):
    timestamp = datetime.strptime(val, '%Y/%m/%d %H:%M:%S').timestamp()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def get_current_utc_datetime():
    return datetime.utcfromtimestamp(datetime.now().timestamp()).strftime('%Y-%m-%dT%H:%M:%SZ')
