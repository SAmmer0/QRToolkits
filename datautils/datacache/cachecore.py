#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/22
"""
import logging

from pandas import to_datetime, concat

from datautils.datacache.const import CacheStatus, LOGGER_NAME

# --------------------------------------------------------------------------------------------------
# 预处理
logger = logging.getLogger(LOGGER_NAME)

# --------------------------------------------------------------------------------------------------
# 类
class DataView(object):
    '''
    数据缓存类(仅限于日频数据)，对数据获取函数进行包装，统一获取数据的接口
    Parameter
    ---------
    func: function
        获取数据的函数，格式为function(start_time, end_time)->pandas.DataFrame or pandas.Series，返回
        数据的时间区间为(start_time, end_time)，包含边界
    calendar: tdtools.tradingcalendar.TradingCalendar
        交易日历对象
    update_method: string
        更新的方法，目前支持[overlap(完全覆盖), stepbystep(逐步加载)]，overlap会请求更多数据，对于读写比较慢
        的数据不友好，stepbystep是在缓存数据的基础上增量加载
    preload_num: int
        必须为正数，指预加载数据的数量。
        当请求的数据超出当前缓存时，需要加载新的数据，为避免频繁加载，可以预先加载超过请求的数据，该参数表示的为
        超过的交易日的数量
    '''
    def __init__(self, func, calendar, update_method='stepbystep', preload_num=100):
        self._func = func
        self._calendar = calendar
        if update_method not in ['overlap', 'stepbystep']:
            raise ValueError('Unsupported \"update_method\"! Valids are [overlap, stepbystep], '+
                             'you provide {}'.format(update_method))
        self._update_method = update_method
        self._offset = preload_num
        self._data_cache = None    # 数据缓存
        self._extendable = [True, True]    # 数据两端是否可以继续扩展，因为本地数据量的限制会导致有些日期的数据无法获取
        self._cache_start = None    # 缓存数据的开始时间
        self._cache_end = None    # 缓存数据的结束时间

    def _check_data(self, query_start, query_end=None):
        '''
        检查缓存数据是否有缺失，并返回数据缺失的方式
        Parameter
        ---------
        query_start: datetime like
            数据请求的开始时间
        query_end: datetime like
            数据请求的结束时间，该参数为None表示是对时点的判断

        Return
        ------
        result: CacheStatus
        '''
        if self._data_cache is None:
            return CacheStatus.BOTH
        query_start = self._calendar.latest_tradingday(query_start, 'FUTURE')
        if query_end is None:
            query_end = max(self._cache_end, query_start)
        if query_start < self._cache_start:
            if query_end <= self._cache_end:
                return CacheStatus.PAST
            else:
                return CacheStatus.BOTH
        if query_start >= self._cache_start and query_start <= self._cache_end:
            if query_end <= self._cache_end:
                return CacheStatus.ENOUGH
            else:
                return CacheStatus.FUTURE
        if query_start > self._cache_end:
            return CacheStatus.FUTURE

    def _update_cache(self, update_direction, left_date=None, right_date=None):
        '''
        更新数据缓存
        Parameter
        ---------
        updat_direction: CacheStatus
            更新的方向，若为BOTH，则left_date和right_date均不能为None；若为PAST，则left_date不能为None；
            若为FUTURE，则right_date不能为None.
        left_date: datetime like, default None
            左(PAST)目标日期
        right_date: datetime like, default None
            右(FUTURE)目标日期

        Notes
        -----
        left_date与right_date参数不能同为None
        '''
        if left_date is None and right_date is None:
            raise ValueError('Parameter \"left_date\" and \"right_date\" cannot be None at the same time!')
        if update_direction == CacheStatus.ENOUGH:
            logger.warning('[Operation=DataView._update_cache, Info=\"Updating cache when CacheStatus ==  ENOUGH\"]')
            return
        if not any(self._extendable):    # 两边数据均不可再扩展
            logger.warning('[Operation=DataView._update_cache, Info=\"Cache cannot be extended!(start_time={st}, end_time={et})\"]'.
                           format(st=self._cache_start, et=self._cache_end))
            return
        if left_date is not None:
            left_date = self._calendar.shift_tradingdays(left_date, -self._offset)
        if right_date is not None:
            right_date = self._calendar.shift_tradingdays(right_date, self._offset)

        if update_direction == CacheStatus.BOTH:
            self._update_both(left_date, right_date)
        elif update_direction == CacheStatus.PAST:
            self._update_one(left_date, update_direction)
        else:
            self._update_one(right_date, update_direction)


    def _update_both(self, left_date, right_date):
        '''
        对两个时间方向的数据进行更新
        Parameter
        ---------
        left_date: datetime
            左目标交易日
        right_date: datetime
            右目标交易日
        '''
        if self._data_cache is None or self._update_method == 'overlap':
            left_date = left_date if self._extendable[0] else self._cache_start
            right_date = right_date if self._extendable[1] else self._cache_end
            self._data_cache = self._func(left_date, right_date).sort_index(ascending=True)
            # 更新数据时间和扩展状态
            self._cache_start = self._data_cache.index[0]
            self._cache_end = self._data_cache.index[-1]
            self._update_extendable_status(left_date, right_date)
        elif self._update_method == 'stepbystep':
            if self._extendable[0]:
                self._update_one(left_date, CacheStatus.PAST)
            if self._extendable[1]:
                self._update_one(right_date, CacheStatus.FUTURE)

    def _update_one(self, date, update_direction):
        '''
        对一个时间方向的数据进行更新
        Parameter
        ---------
        date: datetime
            目标交易日
        update_direction: CacheStatus
            更新的时间方向，要求仅为[PAST, FUTURE]
        '''
        if update_direction not in [CacheStatus.PAST, CacheStatus.FUTURE]:
            raise ValueError('Improper \"update_direction\"!')
        if update_direction == CacheStatus.PAST:
            start_time = date
            if self._update_method == 'overlap':
                end_time = self._cache_end
            else:
                end_time = self._cache_start
        else:
            if self._update_method == 'overlap':
                start_time = self._cache_start
            else:
                start_time = self._cache_end
            end_time = date

        data = self._func(start_time, end_time)
        if self._update_method == 'overlap':
            self._data_cache = data.sort_index(ascending=True)
        else:
            data = data.reindex(data.index.difference(self._data_cache.index), axis=0)
            self._data_cache = concat([data, self._data_cache], axis=0).sort_index(ascending=True)
        # 更新数据时间和扩展状态
        if update_direction == CacheStatus.PAST:
            self._cache_start = self._data_cache.index[0]
            self._update_extendable_status(left_date=date)
        else:
            self._cache_end = self._data_cache.index[-1]
            self._update_extendable_status(right_date=date)

    def _update_extendable_status(self, left_date=None, right_date=None):
        '''
        对数据扩展状态进行更新，仅在每次更新了_cache_start和_cache_end后调用
        Parameter
        ---------
        left_date: datetime, default None
            左目标交易日，None表示不对左边界数据进行检测
        right_date: datetime
            右目标交易日，None表示不对右边界数据进行检测
        '''
        if left_date is None and right_date is None:
            raise ValueError('Parameter \"left_date\" and \"right_date\" cannot be None at the same time!')
        if self._extendable[0] and left_date is not None:
            if left_date < self._cache_start:
                self._extendable[0] = False
        if self._extendable[1] and right_date is not None:
            if right_date > self._cache_end:
                self._extendable[1] = False


    def get_csdata(self, date):
        '''
        获取横截面数据
        Parameter
        ---------
        date: datetime like
            数据的日期，若为非交易日或者数据超出限制会触发KeyError

        Return
        ------
        out: pandas.Series or data
            返回数据的形式取决于提供数据的函数。若提供数据的函数返回值为pandas.DataFram，则返回pandas.Series，
            反之返回具体数据
        '''
        if not self._calendar.is_tradingday(date):
            raise KeyError('Parameter \"date\" must be a trading day!')
        date = to_datetime(to_datetime(date).strftime('%Y-%m-%d'))
        update_direction = self._check_data(date)
        if update_direction != CacheStatus.ENOUGH:
            self._update_cache(update_direction, date, date)
        return self._data_cache.loc[date]

    def get_tsdata(self, start_time, end_time):
        '''
        获取时间序列数据(包含边界)
        Parameter
        ---------
        start_time: datetime like
        end_time: datetime like

        Return
        ------
        out: pandas.Series or pandas.DataFrame
            返回数据的形式取决于提供数据的函数。若提供数据的函数返回值为pandas.DataFram，则返回pandas.DataFrame，
            反之返回pandas.Series
        '''
        start_time = to_datetime(to_datetime(start_time).strftime('%Y-%m-%d'))
        end_time = to_datetime(to_datetime(end_time).strftime('%Y-%m-%d'))
        if start_time >= end_time:
            raise ValueError('Improper time parameter order!')
        update_direction = self._check_data(start_time, end_time)
        if update_direction != CacheStatus.ENOUGH:
            self._update_cache(update_direction, left_date=start_time, right_date=end_time)
        mask = (self._data_cache.index <= end_time) & (self._data_cache.index >= start_time)
        return self._data_cache.loc[mask]
