#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/28

该模块主要收集一些不容易分类的与时间处理相关的小功能函数
"""
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

__all__ = ['trans_date']
