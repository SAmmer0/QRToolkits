#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/20
"""
import pandas as pd

from datatoolkits import load_pickle
import dateshandle
from tdtools.tradingcalendar import TradingCalendar

TD_PATH =  r"E:\GeneralLib\CONST_DATAS\tradingDays.pickle"
td_data = load_pickle(TD_PATH)
trading_times = (('09:30', '11:30'), ('13:00', '15:00'))

sse_calendar = TradingCalendar(td_data, trading_times)

# 交易日计数测试
start_time = '2017-01-01'
end_time = '2018-03-02'
mod_cnt = sse_calendar.count(start_time, end_time, 'both')
old_cnt = dateshandle.tds_count(start_time, end_time)
assert mod_cnt == old_cnt

# 交易日区间测试
start_time = '2016-03-01'
end_time = '2017-11-03'
mod_tds = sse_calendar.get_tradingdays(start_time, end_time, include_type='both')
old_tds = dateshandle.get_tds(start_time, end_time)
assert mod_tds == old_tds

# 周期目标测试
start_time = '2017-01-01'
end_time = '2018-02-27'
targets = sse_calendar.get_cycle_targets(start_time, end_time,
                                        freq='MONTHLY',
                                        target='LAST')
print(targets)
targets = sse_calendar.get_cycle_targets(start_time, end_time,
                                         freq='MONTHLY',
                                        target='FIRST')
print(targets)

# 日期偏移测试
date1 = '2018-03-20'
date2 = '2018-03-18'
print(sse_calendar.shift_tradingdays(date1, 1))
print(sse_calendar.shift_tradingdays(date1, -3))
print(sse_calendar.shift_tradingdays(date2, 1))
print(sse_calendar.shift_tradingdays(date2, -5))
print(sse_calendar.latest_tradingday(date1, 'PAST'))
print(sse_calendar.latest_tradingday(date1, 'FUTURE'))
print(sse_calendar.latest_tradingday(date2, 'PAST'))
print(sse_calendar.latest_tradingday(date2, 'FUTURE'))

# 交易时间判断测试
time1 = pd.to_datetime('2018-03-20 09:35:05')
time2 = pd.to_datetime('2018-03-20 16:35')
print(sse_calendar.is_tradingtime(time1))
print(sse_calendar.is_tradingtime(time2))
