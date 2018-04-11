#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/28

该模块主要收集一些不容易分类的与时间处理相关的小功能函数
"""
import time
from functools import wraps
import datetime as dt
from itertools import cycle

from pandas import to_datetime


def trans_date(*args):
    '''
    对日期进行标准化，转化为pandas.TimeStamp类型，并且将时间重置为0时0分0秒

    Parameter
    ---------
    args: iterable
        元素为datetime like

    Return
    ------
    out: datetime or tuple of datetime
        若len(args)==1，则返回datetime；其他情况返回元组形式
    '''
    result = []
    for d in args:
        d = to_datetime(to_datetime(d).strftime('%Y-%m-%d'))
        result.append(d)
    if len(result) == 1:
        out = result[0]
    else:
        out = tuple(result)
    return out


def timeit_wrapper(func):
    '''
    装饰器，用于打印函数运行的时间

    Parameter
    ---------
    func: function(*args, **kwargs)

    Return
    ------
    wrapped_func: function(*args, **kwargs)
        经过包装的函数，自动打印函数运行的时间
    '''
    @wraps(func)
    def inner(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print('{func} running time: {t:.4f}ms.'.format(
            func=func.__name__, t=(end_time - start_time) * 1000))
        return result
    return inner


def get_last_rpd_date(dates, findmax=True):
    '''
    计算给定的日期序列中最大(或者最小)的季报或者年报日期

    Parameter
    ---------
    dates: iterable
        元素为datetime获取其子类
    ascending: boolean
        True表示查找最大的报告期，False表示查找最小的报告期

    Return
    ------
    out: datetime
    '''
    rpt_dates = ['03-31', '06-30', '09-30', '12-31']
    for date in sorted(dates, reverse=findmax):
        if date.strftime('%m-%d') in rpt_dates:
            return date
    else:
        return None


def generate_rpd_series(date, n):
    '''
    根据给定的报告期生成连续的(过往的)报告期序列

    Parameter
    ---------
    date: datetime or subclass
        该日期必须为报告期
    n: int
        生成的序列长度

    Return
    ------
    out: list
        长度为n的报告期序列，按照降序排列，包含参数date
    '''
    rptd = ['12-31', '09-30', '06-30', '03-31']
    m = int(date.month / 3)
    od = rptd[4-m:] + rptd[:4-m]
    out = []
    for i, d in zip(range(-m, -m+n), cycle(od)):
        out.append(dt.datetime.strptime(str(date.year-i//4-1)+'-'+d, '%Y-%m-%d'))
    return out
