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
class Rule(object):
    '''
    标的筛选规则类
    提供以下功能：
    on_time: 在给定的时间，对给定的标的池进行筛选，然后返回筛选结果和筛选状态(用于表明当前筛选是否启用)

    Parameter
    ---------
    datasources: pitdata.DataGetterCollection
        用于计算的相关数据源
    filter_func: function
        筛选标的用的函数，格式为function(time: datetime, datasources: pitdata.DataGetterCollection, secu_pool: iterable)->list of symbol
    scheduler: RSchedule
        用于设置规则类的启用时间
    '''
    def __init__(self, datasources, filter_func, scheduler):
        self._scheduler = scheduler
        self._filter = filter_func
        self._datasources = datasources

    def on_time(self, time, secu_pool):
        '''
        在每个运行的时间点调用，如果当前时间点符合scheduler设定的时间，则开始依照规则进行计算

        Parameter
        ---------
        time: datetime like
            当前运行的时间
        secu_pool: iterable
            筛选的标的池

        Return
        ------
        enabled: boolean
            用于标记当前规则是否被启用，True表示被启用，False反之
        result: list
            筛选结果，如果该规则未被启用，则直接返回传入的secu_pool
        '''
        if self._scheduler.is_time(time):
            result = self._filter(time, self._datasources, secu_pool)
            enabled = True
        else:
            result = list(secu_pool)
            enabled = False
        return enabled, result
