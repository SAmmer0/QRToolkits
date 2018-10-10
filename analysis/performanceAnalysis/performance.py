#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-10-09 14:43:30
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from math import sqrt

import pandas as pd
import numpy as np

from qrtutils import validation_checker
# --------------------------------------------------------------------------------------------------
# 指标计算类


class Indicator(object):
    '''
    分析指标计算器类
    可直接通过obj(nav, bnav)调用
    提供_mod_params(*args, **kwargs)用于修改func中的其他参数

    Parameter
    ---------
    func: function(nav, bnav, *args, **kwargs)->tuple
        指标计算函数，若bnav为None表示没有基准，需要计算函数自行处理
    args: tuple
        传入到func中的除nav和bnav外的位置参数
    kwargs: dictionary
        传入到func中的关键字参数

    Example
    -------
    >>> indicator1 = Indicator(test_func, *args, **kwargs)
    >>> res = indicator1(nav, bnav)
    '''

    def __init__(self, func, *args, **kwargs):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def _mod_params(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __call__(self, nav, bnav=None):
        return self._func(nav, bnav, *self._args, **self._kwargs)


class IndicatorAnalysorEngine(object):
    '''
    指标分析引擎，用于提供指标分析接口
    通常不需要手动创建该类的实例，而是采用工厂函数创建

    Parameter
    ---------
    indicator_config: dictionary
        格式为{indicator_name: Indicator}

    Example
    -------
    >>> iae = IndicatorAnalysorEngine({'ret': ret_indicator, 'maxdrawndown': mdd_indicator})
    >>> ret = iae.ret(nav, bnav)
    >>> mdd = iae.maxdrawndown(nav, bnav)
    '''

    def __init__(self, indicator_config):
        self._update_dict(indicator_config)
        self._fields = list(indicator_config.keys())

    def _update_dict(self, indicator_config):
        '''
        将indicator_config中的指标计算器添加为分析引擎的属性

        Parameter
        ---------
        indicator_config: dictionary
            格式为{indicator_name: Indicator}
        '''
        self.__dict__.update(indicator_config)

    def apply_indicators(self, nav, bnav=None, field=None):
        '''
        计算所有给定的指标

        Parameter
        ---------
        nav: pandas.Series
            策略净值
        bnav: pandas.Series, default None
            基准净值，默认None表示以没有基准(这种情况由计算函数自行处理)
        field: iterable, default None
            指标名称域，默认None表示所有指标

        Return
        ------
        result: dictionary
            {indicator_name: result}
        '''
        if field is None:
            field = self._fields
        result = {}
        for indicator in field:
            result[indicator] = getattr(self, indicator)(nav, bnav)
        return result

    def list_indicator(self):
        '''
        列出当前所有的指标

        Return
        ------
        indicators_name_list: list
        '''
        return list(self._fields)

# --------------------------------------------------------------------------------------------------
# 工厂类


class IAEFactory(object):
    '''
    用于生成IndicatorAnalysorEngine的工厂类

    提供以下接口：
    make_iae: 用于根据给定配置生成分析引擎
    get_default_iae: 获取默认的分析引擎
    list_indicator: 辅助函数，列举所有的指标

    Parameter
    ---------
    iae_config: dictionary
        格式为{}
    '''

    def __init__(self, default_iae_config):
        self._iae_config = default_iae_config

    def make_iae(self, config):
        '''
        根据给定的配置创建IndicatorAnalysorEngine

        Parameter
        ---------
        config: dictionary
            格式为{indicator_name: (args, kwargs)}

        Return
        ------
        iae: IndicatorAnalysorEngine
            对部分指标计算器的参数修改后的指标分析引擎
        '''
        default_iae = self.get_default_iae()
        for cfg_name, cfg in config.items():
            getattr(default_iae, cfg_name)._mod_params(*cfg[0], **cfg[1])

    def get_default_iae(self):
        '''
        获取默认的IndicatorAnalysorEngine

        Return
        ------
        iae: IndicatorAnalysorEngine
            返回默认的分析引擎
        '''
        return IndicatorAnalysorEngine(self._iae_config)

    def list_indicator(self):
        '''
        列出当前分析引擎中所提供的分析指标名称

        Return
        ------
        indicators_name_list: list
            分析指标列表
        '''
        return list(self._iae_config.keys())

# --------------------------------------------------------------------------------------------------
# 辅助工具


def nav2ret(nav, method='log'):
    '''
    将净值转换为收益率

    Parameter
    ---------
    nav: pandas.Series
        原始净值数据
    method: string, default 'log'
        计算收益率的方法，提供[plain, log]两种，分别表示单利和连续复利
    Return
    ------
    ret: pandas.Series

    Notes
    -----
    第一期的收益被重置为0
    '''
    validation_checker(['log', 'plain'])(method)
    if method == 'log':
        return np.log(nav).diff().fillna(0)
    else:
        return nav.pct_change().fillna(0)
# --------------------------------------------------------------------------------------------------
# 指标计算模板
# 最终收益


def total_return(nav, bnav, method='plain'):
    '''
    计算期间总收益

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值，无用处
    method: string, default 'log'
        计算收益的方法，提供[plain, log]两种

    Return
    ------
    ret: tuple(ret, )
    '''
    validation_checker(['log', 'plain'])(method)
    if method == 'log':
        nav = np.log(nav)
        return (nav.iloc[-1] - nav.iloc[0], )
    else:
        return (nav.iloc[-1] / nav.iloc[0] - 1, )


def comparable_return(nav, bnav, freq=250, method='plain'):
    '''
    可比收益，即将不同频率的数据换算到同一频率后得到的收益，例如将收益年化

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'log'
        收益计算方式，提供[plain, log]两种

    Return
    ------
    ret: tuple(ret, )

    Notes
    -----
    plain模式下年化收益的计算方式为 ret = (1 + total_ret)**(freq/len) - 1
    log模式下年化收益的计算方式为 ret = log_total_ret * freq / len
    '''
    tret = total_return(nav, bnav, method)[0]
    if method == 'log':
        return (tret * freq / len(nav), )
    else:
        return ((1 + tret)**(freq / len(nav)) - 1, )


def comparable_vol(nav, bnav, freq=250, method='plain'):
    '''
    可比波动率，即将不同频率的数据换算到同意频率后得到的波动率，例如将波动率年化

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    freq: int, default 250
        策略精致的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'log'
        收益计算方式，提供[plain, log]两种

    Return
    ------
    vol: tuple(vol, )

    Notes
    -----
    adjusted_vol = sqrt((init_vol**2 + (1 + init_ret)**2)**ret_freq - (1+init_ret)**(2*ret_freq))
    其中，init_vol和init_ret分别为样本波动率和样本平均收益率
    '''
    rets = nav2ret(nav, method)
    init_vol = np.std(rets)
    init_ret = np.mean(rets)
    return (sqrt((init_vol**2 + (1 + init_ret)**2)**freq - (1 + init_ret)**(2 * freq)), )


# --------------------------------------------------------------------------------------------------
# 默认指标计算配置
DEFAULT_IAE_CONFIG = {'total_return': Indicator(total_return),
                      'annualized_return': Indicator(comparable_return),
                      'annualized_vol': Indicator(comparable_vol)}
general_iae_factory = IAEFactory(DEFAULT_IAE_CONFIG)
