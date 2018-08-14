#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/13
"""
import logging
from copy import deepcopy

from datautils.datacollection.const import LOGGER_NAME
# --------------------------------------------------------------------------------------------------
# 预处理
logger = logging.getLogger(LOGGER_NAME)
# --------------------------------------------------------------------------------------------------
# Collection类
class DataGetterCollection(object):
    '''
    数据获取器集合类
    用于将应用中要使用的不同的数据放在一个集合中，方便进行统一调用
    使用方法：
    创建集合对象，使用字典初始化或者通过类似字典的方式添加数据获取器

    Parameter
    ---------
    data_getters: dictionary, default None
        格式为{data_name: getter}，getter通常假定为DataView或者与其有相同的get_tsdata以及get_csdata接口
        的对象
    '''
    def __init__(self, data_getters=None):
        if data_getters is None:
            self._data = {}
        else:
            self._data = deepcopy(data_getters)

    def __len__(self):
        '''
        内部数据获取器数量
        '''
        return len(self._data)

    def __getitem__(self, name):
        '''
        通过名称获取内部数据获取器

        Parameter
        ---------
        name: string
            数据名称

        Return
        ------
        dv: DataView or the like
            返回的对象必须具有get_csdata和get_tsdata两个接口
        '''
        if name in self._data:
            return self._data[name]
        else:
            raise KeyError('{} cannot be found!'.format(name))

    def __setitem__(self, name, dv):
        '''
        向集合中添加或者修改数据获取器

        Parameter
        ---------
        name: string
            数据名称
        dv: DataView or the like
            DataView对象或者其他具有get_csdata以及get_tsdata两个接口的对象
        '''
        try:
            if name in self._data:
                logger.info('[Operation=DataGetterCollection.__setitem__, Info=\'Modify existing data getter(name={}).\''.format(name))
            self._data[name] = dv
        except Exception as e:
            logger.exception(e)

    def __iter__(self):
        '''
        迭代器，用于返回内置的数据获取器，迭代顺序不确定
        '''
        return iter(self._data.values())

    def list_data(self):
        '''
        列举当前集合中的所有数据名称

        Return
        ------
        out: list
        '''
        return list(self._data.keys())
