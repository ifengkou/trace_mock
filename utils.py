# -*- encoding: utf8 -*-
'''
author: shenlongguang<https://github.com/ifengkou>
Date: 2021-09-04 10:42:57
'''

import datetime
import time

def is_number(s):
    try:
        float(s)
        return True
    except:
        return False


def is_int(s):
    try:
        int(s)
        return True
    except:
        return False


def to_date(dt, format='%Y-%m-%d %H:%M:%S'):
    try:
        return datetime.datetime.strptime(dt, format)
    except:
        return datetime.datetime.strptime(dt, '%Y-%m-%d')

def current_time():
    t = time.time()
    return int(round(t * 1000))

def current_time_str():
    t = datetime.datetime.now()
    return time.strftime('%Y-%m-%d %H:%M:%S',)

def timestamp_to_ftime(timestamp,format='%Y-%m-%d %H:%M:%S'):
    """
    毫秒timestamp 格式化成时间字符串，具体到秒
    """
    dt = time.localtime(timestamp/1000)
    value = time.strftime(format, dt)
    return value

def mils_to_day(mills):
    """
    毫秒mills 格式化成日期 整形
    """
    dt = time.localtime(mills/1000)
    str_day = time.strftime('%Y%m%d', dt)
    _day = int(str_day)
    return _day

def to_unix_timestamp(dt,format='%Y-%m-%d %H:%M:%S'):
    _datetime = to_date(dt)
    s = time.mktime(_datetime.timetuple())
    return int(round(s * 1000))

def convert_data_type(data_type,ele):
    try:
        if data_type == 'string':
            return str(ele)
        elif data_type == 'int':
            return int(ele)
        elif data_type == 'float':
            return float(ele)
        elif data_type == 'datetime':
            return to_date(ele)
        elif data_type == 'bool': 
            if 'true' == str(ele).lower() or  '1' == str(ele):
                return True
            elif 'false' == str(ele).lower() or '0' == str(ele):
                return False
    except:
        raise ValueError(f"Can't parse `{ele}` `{data_type}`")
    # 暂时不支持转成array类型
    return ele