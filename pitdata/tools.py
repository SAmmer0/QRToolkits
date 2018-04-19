#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/17
"""

from datautils import DataView
from tdtools import get_calendar
from pitdata.query import query

# --------------------------------------------------------------------------------------------------
# 数据缓存获取类
class PITDataCache(object):
    '''
    工厂类，用于生成和获取数据缓存
    数据缓存的唯一标志是：数据名称+数据的预加载数量

    Example
    -------
    >>> from pitdata.tools import pitcache_getter
    >>> beta_cache = pitcache_getter('BETA', 100)    # pitcache_getter(data_name, preload_num)
    >>> data = beta_cache.get_csdata('2018-04-19')
    '''
    __cache = {}
    def __call__(self, name, preload_num):
        '''
        加载给定名称和预加载数量的数据

        Parameter
        ---------
        name: string
            数据名称
        preload_num: int
            预加载数据的数量，必须为正整数

        Return
        ------
        cache: datautils.DataView
        '''
        if preload_num < 0:
            raise ValueError('Parameter \"preload_num\" cannot be negetative!')
        if not isinstance(preload_num, int):
            raise TypeError('The type of parameter \"preload_num\" must be int!')
        cache_name = name + '_{:03d}'.format(preload_num)    # 缓存的唯一标志为name+preload_num，例如"BETA_100"
        if cache_name in self.__cache:
            return self.__cache[cache_name]
        else:
            func = lambda st, et: query(name, st, et)
            cache = DataView(func, get_calendar('stock.sse'), preload_num=preload_num)
            self.__cache[cache_name] = cache
            return cache

pitcache_getter = PITDataCache()    # 此处并未将类设计成单例模式，考虑到可以由很多独立的缓存，但提供一个常用的缓存
