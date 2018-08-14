#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/14
"""
from tdtools import get_calendar
# --------------------------------------------------------------------------------------------------
# 时间表类及常用实例
class RSchedule(object):
    '''
    计划表类工厂类，用于生成具有不同的时间判断功能的对象

    Parameter
    ---------
    time_condition: function or callable
        时间判断函数，格式为function(rtime)->boolean
    '''
    def __init__(self, time_condition):
        self._time_condition = time_condition

    def is_time(self, rtime):
        '''
        判断当前是否为条件要求的时间点

        Parameter
        ---------
        rtime: datetime or the like
            需要判断的时间

        Return
        ------
        result: boolean
            True表示时间符合条件
        '''
        return self._time_condition(rtime)

ssetd_scheduler = RSchedule(get_calendar('stock.sse').is_tradingday)  # 股票交易日时间计划表
sseme_scheduler = RSchedule(lambda t: get_calendar('stock.sse').is_cycle_target(t, 'MONTHLY', 'LAST'))  # 股票交易日月末计划表

# --------------------------------------------------------------------------------------------------
# Rule
