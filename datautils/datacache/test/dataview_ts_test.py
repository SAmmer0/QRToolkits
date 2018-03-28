#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/28
"""
import numpy as np

from fmanager import query
from tdtools import get_calendar
from datautils.datacache.cachecore import DataView

test_dates = [('2015-04-16', '2015-05-07'), ('2016-06-01', '2017-01-01'), ('2017-11-01', '2018-03-01'), ('2014-01-01', '2014-05-01')]

def get_df(start_time, end_time):
    return query('CLOSE', (start_time, end_time))
dv_df = DataView(get_df, get_calendar('stock.sse'))

for date in test_dates:
    tmp = dv_df.get_tsdata(*date).fillna(-1000)
    data_cpr = query('CLOSE', date).fillna(-1000)
    assert np.all(np.all(np.isclose(tmp, data_cpr), axis=1))
    print(dv_df._cache_start, dv_df._cache_end, dv_df._extendable)
assert len(dv_df._data_cache) == get_calendar('stock.sse').count(dv_df._cache_start, dv_df._cache_end)
