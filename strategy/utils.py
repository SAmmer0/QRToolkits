#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/14
"""

from copy import deepcopy
import logging

from strategy.const import LOGGER_NAME
# --------------------------------------------------------------------------------------------------
# 日志设置
logger = logging.getLogger(LOGGER_NAME)

# --------------------------------------------------------------------------------------------------
# 类
class Position(object):
    '''
    持仓类，用于存储当前的标的持仓
    可通过字典初始化，该类也提供类似字典的添加修改数据的方式，也可以通过update函数进行批量更新
    该类还提供to_dict方法将持仓以字典的形式导出

    Parameter
    ---------
    pos: dictionary, default None
        使用字典对持仓对象进行初始化，字典格式为{secu_name: num}
    '''
    def __init__(self, pos=None):
        if pos is not None:
            self._position = deepcopy(pos)
        else:
            self._position = {}

    def update(self, **kwargs):
        '''
        对当前持仓进行批量更新

        Parameter
        ---------
        kwargs: dictionary
            使用键值模式数据的参数，格式为secu_name=num

        Return
        ------
        result: boolean
            True表示成功更新
        '''
        try:
            self._position.update(**kwargs)
        except Exception as e:
            logger.exception(e)
            return False
        return True

    def to_dict(self):
        '''
        将持仓以字典的形式导出，格式为{secu_name: num}

        Return
        ------
        result: dictionary
            内部持仓数据的副本
        '''
        return deepcopy(self._position)

    def to_pdseries(self):
        '''
        将持仓以pandas.Series的形式导出

        Return
        ------
        result: pandas.Series
            index为标的名称，value为标的持仓
        '''
        return pd.Series(self._position)

    def __setitem__(self, name, value):
        '''
        以字典的形式设置数据

        Parameter
        ---------
        name: string
            标的名称
        value: float
            标的的仓位
        '''
        self._position[name] = value

    def __getitem__(self, name):
        '''
        通过键名获取仓位值

        Return
        ------
        pos: float
        '''
        if name not in self._position:
            raise KeyError('{} cannot be found!'.format(name))
        return self._position[name]
