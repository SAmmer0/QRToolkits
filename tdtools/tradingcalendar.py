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

from pandas import to_datetime, Timedelta

from tdtools.const import CONFIG, LOGGER_NAME, Frequency, TargetSign
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
        每个交易日内的交易的起始时间，元素为tuple。例如，上交所股票市场交易时间为
        (('09:30', '11:30'), ('13:00', '15:00'))
    '''
    def __init__(self, data, trading_times):
        self._data = sorted(to_datetime(data))
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

    def __date_pretreatment(self, *args):
        '''
        对日期进行预处理：将日期转换为pd.TimeStamp类型并检查给定的日期是否在数据范围内

        Parameter
        ---------
        args: iterable
            元素为datetime like

        Return
        ------
        result: tuple
            转化后的日期数据(与参数顺序相同，且hour、minute、second都会被重置为0)
        '''
        result = []
        for d in args:
            d = to_datetime(d)
            d = to_datetime(d.strftime('%Y-%m-%d'))
            if d < self._data[0] or d > self._data[-1]:
                raise ValueError("Date({}) exceed data range!".format(d))
            result.append(d)
        return tuple(result)

    def get_cycle_targets(self, start_time, end_time, freq=Frequency.MONTHLY, target=TargetSign.LAST):
        '''
        获取一段时间内(包含起始和终止日期)的交易日按照给定周期分类后的目标日期序列
        这里的目标日期是指每个周期内的特殊日期，比如说第一个或者最后一个交易日

        Parameter
        ---------
        start_time: datetime like
            起始时间
        end_time: datetime like
            终止时间
        freq: string or Frequency(Enum)
            日期分类频率，[WEEKLY, MONTHLY, YEARLY]
        target: string or TargetSign(Enum)
            目标标记，[FIRST, LAST]

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
        start_time, end_time = self.__date_pretreatment(start_time, end_time)
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
        start_time, end_time = self.__date_pretreatment(start_time, end_time)
        out = [d for d in self._data if d >= start_time and d <= end_time]
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

    def count(self, start_time, end_time, include_type='both'):
        '''
        获取给定时间区间内的交易日的数量

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
        out: int
        '''
        start_time, end_time = self.__date_pretreatment(start_time, end_time)
        return len(self.get_tradingdays(start_time, end_time, include_type))

    def shift_tradingdays(self, date, offset):
        '''
        计算给定的日期推移一定数量的交易日得到的结果，推移的规则如下：
        offset > 0表示往未来推移，offset < 0表示往过去推移，offset不能等于0(这种行为会导致不确定的结果)
        例如，offset=1表示计算与其相邻的下一个(未来)交易日(即>=date且为交易日)，其他以此类推
        同样，offset=-1表示计算与其相邻的上一个(过去)交易日，其他类推

        Parameter
        ---------
        date: datetime like
            锚定的日期
        offset: int
            推移的交易日数量

        Return
        ------
        out: datetime
        '''
        date = self.__date_pretreatment(date)[0]
        if offset == 0:
            raise ValueError('Illegal value(0) for \"offset\" argument!')
        if offset > 0:
            data = [d for d in self._data if d > date]
            return data[offset - 1]
        else:
            data = [d for d in self._data if d < date]
            return data[offset]


    def is_tradingday(self, date):
        '''
        判断给定的日期是否是交易日

        Parameter
        ---------
        date: datetime like

        Return
        ------
        result: boolean
        '''
        date = self.__date_pretreatment(date)[0]
        if date < self._data[0] or date > self._data[-1]:
            raise ValueError('Date parameter({d}) exceeds data range(from {s} to {e})!'.
                             format(d=date, s=self._data[0], e=self._data[-1]))
        return date in self._data

    def latest_tradingday(self, date, direction):
        '''
        获取距离给定日期最近的交易日(date为交易日，则直接返回date)，主要用于弥补shift_tradingdays
        不能将offset设置为0的缺陷

        Parameter
        ---------
        date: datetime like
            锚定日期
        direction: string, default FUTURE
            推断的方向，仅支持[PAST, FUTURE]，仅在date为非交易日时起作用，PAST表示计算与
            date相邻且小于date的交易日，FUTURE表示计算与date相邻且大于date的交易日
        Return
        ------
        out: datetime
        '''
        date = self.__date_pretreatment(date)[0]
        if direction not in ['PAST', 'FUTURE']:
            raise ValueError("Illegal direction parameter({}),".format(direction) +
                             "Only [PAST, FUTURE] are supported!")
        if self.is_tradingday(date):
            return date
        if direction == 'PAST':
            return self.shift_tradingdays(date, -1)
        else:
            return self.shift_tradingdays(date, 1)

    def is_cycle_target(self, date, freq, target):
        '''
        判断给定的日期是否是某个周期下的特殊日期，同get_cycle_targets中定义的日期

        Parameter
        ---------
        date: datetime like
            判断的日期
        freq: string or Frequency(Enum)
            日期分类频率，[WEEKLY, MONTHLY, YEARLY]
        target: string or TargetSign(Enum)
            目标标记，[FIRST, LAST]

        Return
        ------
        result: boolean
        '''
        date = self.__date_pretreatment(date)[0]
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
        return date in cache

    def is_tradingtime(self, t):
        '''
        判断给定的时间是否为交易时间，条件包含两个:
        当天日期是交易日，当前时间在交易时间内
        
        Parameter
        ---------
        t: datetime

        Return
        ------
        result: boolean
        '''
        if not self.is_tradingday(t):    # 非交易日
            return False

        def concate_time(date, tt):
            # 将给定日期与交易起始时间连接
            return to_datetime(date.strftime('%Y-%m-%d ') + tt, format='%Y-%m-%d %H:%M')

        for period in self._trading_times:
            start_time = concate_time(t, period[0])
            end_time = concate_time(t, period[1])
            if t >= start_time and t <= end_time:
                return True
        else:
            return False

