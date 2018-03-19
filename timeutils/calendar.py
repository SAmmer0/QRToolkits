#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/16
"""
import logging
from itertools import groupby

from pandas import to_datetime

from timeutils.const import CONFIG, LOGGER_NAME, Frequency, TargetSign
from database import Database

# --------------------------------------------------------------------------------------------------
# 设置预处理选项
logger = logging.getLogger(LOGGER_NAME)

# --------------------------------------------------------------------------------------------------
# 类
class TradingCalendar(object):
    '''
    交易日历类，用于处理与交易日计算有关的问题
    提供的接口包含：

    Parameter
    ---------
    data: iterable
        交易日数据，元素为交易日
    trading_times: tuple
        每个交易日内的交易的起始时间，元素为namedtuple。例如，上交所股票市场交易时间为
        ((start_time='09:30', end_time='11:30'), (start_time='13:00', end_time='15:00'))
    '''
    def __init__(self, data, trading_times):
        self._data = data
        self._trading_times = trading_times
        self._cache = {}
        self._target_func_map = {TargetSign.LAST: lambda x: max(x),
                                 TargetSign.FIRST: lambda x: min(x)}
        self._freq_fmt_map = {Frequency.MONTHLY: '%Y-%m',
                              Frequency.YEARLY: '%Y',
                              Frequency.WEEKLY: '%Y-%W'}

    def __calculate_target_tds(self, fmt, target_function):
        '''
        按照给定的频率对交易日进行分类(例如，按照月度、周度或者年度)，然后从每个分组中选出一个日期作为目标日，
        并将所有分组的目标日构成的序列存入缓存中
        Parameter
        ---------
        fmt: string
            对交易日进行分组的形式，要求为strptime可接受的形式，例如按照月度分类为%Y-%m
        target_function: function
            筛选每个分组内目标日的函数，要求格式为target_function(list of dates)->target_date

        Return
        ------
        result: list
            目标交易日序列
        '''
        groups = groupby(self._data, lambda x: x.strftime(fmt))
        result = []
        for _, date_group in groups:
            result.append(target_function(list(date_group)))
        return result

    def get_cycle_targets(self, start_time, end_time, freq=Frequency.MONTHLY, target=TargetSign.LAST):
        '''
        获取一段时间内(包含起始和中止日期)的交易日按照给定周期分类后的目标日期序列
        这里的目标日期是指每个周期内的特殊日期，比如说第一个或者最后一个交易日
        Parameter
        ---------
        start_time: datetime like
            起始时间
        end_time: datetime like
            终止时间
        freq: string or Frequency(Enum)
            时间分类频率
        target: string or TargetSign(Enum)
            目标标记

        Return
        ------
        result: list
        '''
        if isinstance(freq, str):
            freq = Frequency[freq]
        if isinstance(target, str):
            target = TargetSign[target]
        if freq not in self._cache or target not in self._cache[freq]:    # 缓存中没有相关数据
            cache = self.__calculate_target_tds(self._freq_fmt_map[freq],
                                                self._target_func_map[target])
            cur_cache = self._cache.setdefault(freq, {})
            cur_cache[target] = cache
        else:
            cache = self._cache[freq][target]
        result = sorted(d for d in cache if d >= start_time and d <= end_time)
        return result

    def get_tradingdays(self, start_time, end_time, include_type='both'):
        '''
        获取给定时间区间内的交易日
        Parameter
        ---------
        start_time: datetime like
            起始时间
        end_time: datetime like
            终止时间
        include_type: string
            起止时间包含类型，分为both = [], left = [), right = (]

        Return
        ------
        out: list
        '''
        start_time = to_datetime(start_time)
        end_time = to_datetime(end_time)
        out = sorted([d for d in self._data if d >= start_time and d <= end_time])
        if include_type == 'both':
            pass
        elif include_type == 'left':
            if end_time in out:
                out = out[:-1]
        elif include_type == 'right':
            if start_time in out:
                out = out[1:]
        else:
            raise ValueError('Only [left, right, both] include_type are supported, you provide {}'.
                             format(include_type))
        return out
