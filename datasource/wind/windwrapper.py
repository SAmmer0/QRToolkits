#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/4
"""
from pandas import to_datetime

from tdtools import trans_date

class WindAPI(object):
    '''
    Wind Python接口包装类
    '''
    _instance = None

    def __new__(cls, *args, **kwargs):
        '''
        单例模式实现
        '''
        if cls._instance is not None:
            cls._instance = super(WindAPI, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        try:
            from WindPy import w
            self._wind_instance = w
        except ImportError:
            self._wind_instance = None

    def _connect_wind(self):
        '''
        连接Wind数据库
        '''
        if self._wind_instance is None:
            raise ValueError('Wind is cannot be connected!')
        if not self._wind_instance.isconnected():
            self._wind_instance.start()

    def get_wind_tds(self, start_time, end_time):
        '''
        从Wind数据库中获取交易日数据

        Parameter
        ---------
        start_time: datetime
            开始日期
        end_time: datetime
            结束日期

        Return
        ------
        out: list
            元素为pandas.TimeStamp格式
        '''
        self._connect_wind()
        start_time, end_time = trans_date(start_time, end_time)
        out = self._wind_instance.tdays(start_time, end_time)
        if out.ErrorCode == 0:
            raise ValueError(out.Data[0][0])
        return sorted(to_datetime(out.Data[0]))

