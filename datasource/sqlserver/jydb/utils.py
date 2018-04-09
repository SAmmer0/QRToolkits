#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/9
"""

from tdtools import get_calendar

def map2td(data, start_time, end_time, timecol=None, from_now_on=True, fillna=None):
    '''
    将(一系列)数据映射到给定的交易日
    Parameter
    ---------
    data: pandas.DataFrame
        待映射的数据，数据中必须包含时间特征(在数据列中或者在index中)
    start_time: datetime like
        映射结果的开始时间
    end_time: datetime like
        映射结果的结束时间
    timecol: string, default None
        若数据中包含时间列，则需要提供时间列的列名；该参数为None表示时间包含在index中
    from_now_on: boolean
        映射规则。若from_now_on为True，表示给定时点及其后的值等于该时点的值，直至下一个时点；
        若该参数为False，表示仅该时点之后的值等于该时点的值，直至下一个时点。
    fillna: function, default None
        默认为None表示不对缺省数据做填充；若需要填充，对应的格式为{col: function(data)->value}，
        即填充函数接受原数据作为输入，返回一个填充的值

    Return
    ------
    out: pandas.DataFrame
        映射后的数据，返回数据的格式与传入数据相同(即，若index为时间，返回值中index也为时间)
    '''
    mapped_tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    if timecol is not None:
        out = data.set_index(timecol, drop=True)
    out = data.reindex(mapped_tds, method='ffill')
    if not from_now_on:
        out = out.shift(1)
    if timecol is not None:
        out = out.reset_index()
    if fillna:
        for col in fillna:
            func = fillna[col]
            fill_value = func(data)
            out[col] = out[col].fillna(fill_value)
    return out
