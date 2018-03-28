#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/28
"""
import numpy as np

from datautils.datacache.cachecore import DataView
from fmanager import query
from tdtools import get_calendar

def get_df(start_time, end_time):
    return query('CLOSE', (start_time, end_time))

dv_df = DataView(get_df, get_calendar('stock.sse'))

# 横截面数据获取测试
date1 = '2016-03-25'
tmp = dv_df.get_csdata(date1).fillna(-1000)
data_cpr = query('CLOSE', date1).iloc[0, :].fillna(-1000)
assert np.all(np.isclose(tmp, data_cpr))
print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)

date2 = '2017-01-04'
tmp = dv_df.get_csdata(date2).fillna(-1000)
data_cpr = query('CLOSE', date2).iloc[0, :].fillna(-1000)
assert np.all(np.isclose(tmp, data_cpr))
print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)

date3 = '2018-01-04'
tmp = dv_df.get_csdata(date3).fillna(-1000)
data_cpr = query('CLOSE', date3).iloc[0, :].fillna(-1000)
assert np.all(np.isclose(tmp, data_cpr))
print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)

date4 = '2018-03-05'
tmp = dv_df.get_csdata(date4).fillna(-1000)
data_cpr = query('CLOSE', date4).iloc[0, :].fillna(-1000)
assert np.all(np.isclose(tmp, data_cpr))
print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)

date5 = '2014-03-05'
tmp = dv_df.get_csdata(date5).fillna(-1000)
data_cpr = query('CLOSE', date5).iloc[0, :].fillna(-1000)
assert np.all(np.isclose(tmp, data_cpr))
print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)
assert len(dv_df._data_cache) == get_calendar('stock.sse').count(dv_df._cache_start, dv_df._cache_end)
