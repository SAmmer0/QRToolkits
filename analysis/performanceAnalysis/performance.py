#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-10-09 14:43:30
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from math import sqrt
import pdb
from copy import deepcopy
from functools import partial

import pandas as pd
import numpy as np
import scipy.stats as sp_stats

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
        格式为{indicator_name: Indicator}
    '''

    def __init__(self, default_iae_config):
        self._iae_config = default_iae_config
        self._default_iae = None

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
        iea = deepcopy(self.get_default_iae())
        for cfg_name, cfg in config.items():
            getattr(iea, cfg_name)._mod_params(*cfg[0], **cfg[1])
        return iea

    def get_default_iae(self):
        '''
        获取默认的IndicatorAnalysorEngine

        Return
        ------
        iae: IndicatorAnalysorEngine
            返回默认的分析引擎
        '''
        if self._default_iae is None:
            self._default_iae = IndicatorAnalysorEngine(self._iae_config)
        return self._default_iae

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


def nav2ret(nav, method='log', window=1):
    '''
    将净值转换为收益率

    Parameter
    ---------
    nav: pandas.Series
        原始净值数据
    method: string, default 'log'
        计算收益率的方法，提供[plain, log]两种，分别表示单利和连续复利
    window: int, default 1
        计算收益的时间窗口长度

    Return
    ------
    ret: pandas.Series

    Notes
    -----
    第一期的收益被重置为0
    '''
    validation_checker(['log', 'plain'])(method)
    if method == 'log':
        return np.log(nav).diff(window).fillna(0)
    else:
        return nav.pct_change(window).fillna(0)


def ret2nav(ret, method='log'):
    '''
    将收益率转换为净值

    Parameter
    ---------
    ret: pandas.Series
        收益率数据
    method: string, default 'plain'
        转换方式，可选的有[plain, log]

    Return
    ------
    nav: pandas.Series
    '''
    validation_checker(['log', 'plain'])(method)
    if method == 'log':
        return np.exp(ret.cumsum())
    else:
        return ret.add(1).cumprod()


def transform_return_frequency(raw_ret, freq, method='plain'):
    '''
    转换收益的频率

    Parameter
    ---------
    raw_ret: float
        原始收益
    freq: float
        目标收益相对于源收益数据的倍数，例如源数据为月度收益，要转换为年度收益，则freq为12
    method: string, default 'plain'
        收益计算方式，可选为[plain, log]

    Return
    ------
    transed_ret: float
    '''
    validation_checker(['log', 'plain'])(method)
    if method == 'plain':
        return (1 + raw_ret)**freq - 1
    else:
        return raw_ret * freq


def normalize_nav(nav):
    '''
    将净值正则化，即转化为以1为初始值的净值序列

    Parameter
    ---------
    nav: pandas.Series
        原净值序列

    Return
    ------
    transed_nav: pandas.Series
    '''
    return nav / nav.iloc[0]
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
    period_len = len(nav) - 1
    if method == 'log':
        return (transform_return_frequency(tret, freq / period_len, method), )
    else:
        return (transform_return_frequency(tret, freq / period_len, method), )


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
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
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


def max_drawndown(nav, bnav):
    '''
    最大回撤函数

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值

    Return
    ------
    result: tuple(mdd, mdd_start_time, mdd_end_time)
    '''
    cum_max = nav.cummax()
    dd = 1 - nav / cum_max
    mdd = dd.max()
    mdd_end = dd.idxmax()
    mdd_start = nav[nav == cum_max[mdd_end]].index[0]
    return mdd, mdd_start, mdd_end


def max_drawndown_duration(nav, bnav):
    '''
    最大回撤期

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值

    Return
    ------
    result: tuple(mddd, mddd_start_time, mddd_end_time)
    '''
    cum_max = nav.cummax()
    max_duration_nav = cum_max.value_counts().argmax()
    max_duration_period = cum_max.loc[np.isclose(cum_max, max_duration_nav)]
    duration_length = len(max_duration_period)
    duration_start = min(max_duration_period.index)
    duration_end = max(max_duration_period.index)
    return (duration_length, duration_start, duration_end)


def rolling_drawndown(nav, bnav, window=250):
    '''
    滚动回撤

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    window: int, default 250
        滚动窗口的长度

    Return
    ------
    out: (pandas.Series, )
    '''
    rolling_max = nav.rolling(window, min_periods=1).max()
    out = 1 - nav / rolling_max
    return out


def rolling_comparable_return(nav, bnav, freq=250, method='plain', cut_tail=30):
    '''
    滚动起始时间的可比收益，即以每一个交易日作为起始日期计算最终的可比收益

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    method: string, default 'plain'
        收益计算方式，可选为[plain, log]
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    cut_tail: int, default 10
        尾部数据因为时间间隔比较短，容易受极端值的影响，因此需要扣除。该参数表示扣除的尾部的长度

    Return
    ------
    out: (pands.Series, )
    '''
    validation_checker(['plain', 'log'])(method)
    if freq <= 0 or not isinstance(freq, int):
        raise ValueError('freq parameter must be positive integer!')
    if cut_tail <= 0 or not isinstance(freq, int):
        raise ValueError('cut_tail parameter must be positive integer!')
    valid_nav = nav.iloc[:-cut_tail]
    period_time = freq / (len(nav) - np.arange(1, len(valid_nav) + 1))
    if method == 'plain':
        ret = (nav.iloc[-1] / valid_nav)**period_time - 1
    else:
        ret = (np.log(nav.iloc[-1]) - np.log(valid_nav)) * period_time
    return (ret, )


def win_rate(nav, bnav, threshold=0., method='plain'):
    '''
    胜率计算函数

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    threshold: float, default 0
        识别合意收益的阈值，默认为0，大于等于该阈值的数据将被认为是合意收益
    method: string, default 'plain'
        收益计算方式，可选为[plain, log]

    Return
    ------
    wr: (win_rate, )
    '''
    ret = nav2ret(nav, method)
    win_count = (ret >= threshold).sum()
    return (win_count / len(ret))


def raw_beta(nav, bnav, method='plain'):
    '''
    粗略的计算beta

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    method: string, default 'plain'
        收益的计算方式，可选为[plain, log]

    Return
    ------
    beta: (beta, )

    Notes
    -----
    beta = cov(ret, ret_benchmark) / var(ret_benchmark)
    '''
    ret = nav2ret(nav, method)
    ret_benchmark = nav2ret(bnav, method)
    return (np.cov(ret, ret_benchmark)[0][1] / np.var(ret_benchmark), )


def raw_alpha(nav, bnav, rf_rate=0., freq=250, method='plain'):
    '''
    粗略的计算alpha

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准的净值
    rf_rate: flaot, default 0.
        无风险利率，频率与净值频率相同
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'plain'
        收益的计算方式，可选为[plain, log]

    Return
    ------
    alpha: (alpha, )

    Notes
    -----
    alpha = comparable_ret - transed_rf_rate - raw_beta*(comparable_ret_benchmark - rf_rate)
    '''
    comparable_ret = comparable_return(nav, bnav, freq, method)[0]
    comparable_ret_benchmark = comparable_return(bnav, bnav, freq, method)[0]
    rawbeta = raw_beta(nav, bnav, method)[0]
    transed_rf_rate = transform_return_frequency(rf_rate, freq, method)
    return (comparable_ret - transed_rf_rate - rawbeta * (comparable_ret_benchmark - transed_rf_rate), )


def info_ratio(nav, bnav, freq=250, method='plain'):
    '''
    信息比率计算

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'plain'
        收益计算方法， 可选为[plain, log]

    Return
    ------
    infor: (infor, )

    Notes
    -----
    infor = comparable_ret(excess_ret) / comparable_vol(excess_ret)
    '''
    ret = nav2ret(nav, method)
    ret_benchmark = nav2ret(bnav, method)
    excess_ret = ret - ret_benchmark
    comparable_excess_ret = comparable_return(ret2nav(excess_ret, method), None)[0]
    comparable_excess_vol = comparable_vol(ret2nav(excess_ret, method), None, freq, method)[0]
    return (comparable_excess_ret / comparable_excess_vol, )


def sharp_ratio(nav, bnav=None, freq=250, method='plain', *, rf_rate=0.):
    '''
    计算夏普比率

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series, default None
        基准净值，此处没有作用，仅为占位符
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'plain'
        收益计算方法，可选的包含[plain, log]
    rf_rate: float, default 0.
        无风险利率，仅能使用键值方式设置，频率与净值数据相同

    Return
    ------
    sr: (sr, )

    Notes
    -----
    sr = compararable_ret(ret - rf_rate) / comparable_vol(ret)
    '''
    bnav = ret2nav(pd.Series(rf_rate * np.ones_like(nav), index=nav.index))
    return info_ratio(nav, bnav, freq, method)


def oneside_vol(nav, bnav, freq=250, method='plain', threshold=0., direction=-1):
    '''
    单向波动率，即上行波动率或者下行波动率

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'plain'
        计算收益率的方法，可选有[log, plain]
    threshold: double, default 0.
        识别上行或者下行收益的阈值，收益的频率与freq要匹配
    direction: int, default -1
        波动率方向，可选为[-1, 1]，其中-1表示下行，1表示上行

    Return
    ------
    dv: (dv, )

    Notes
    -----
    dv = sqrt(freq / len(ret) * sum(min(ret - threshold, 0)**2))
    '''
    validation_checker([-1, 1])(direction)
    ret = nav2ret(nav, method)
    valid_ret = ret - threshold
    valid_ret.loc[direction * valid_ret <= 0] = 0
    return (np.sqrt(valid_ret.dot(valid_ret) * freq / len(nav)), )


def sortino_ratio(nav, bnav, freq=250, method='plain', rf_rate=0., threshold=None):
    '''
    索提诺比率

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    freq: int, default 250
        策略净值的频率，即低频数据相对于高频数据的时间倍数，例如一般一年约有250个交易日，则日频数据计算年化数据的频率为250
    method: string, default 'plain'
        计算收益率的方法，可选有[log, plain]
    rf_rate: float, default 0.
        无风险利率，频率与净值数据相同
    threshold: float, default None
        下行波动率的识别阈值，默认为None，表示该阈值等于无风险利率

    Return
    ------
    sr: (sr, )

    Notes
    -----
    sr = comparable_ret(excess_ret) / downside_vol
    '''
    ret = nav2ret(nav, method)
    excess_ret = ret - rf_rate
    cpr_ret = comparable_return(ret2nav(excess_ret, method), bnav, freq, method)[0]
    if threshold is None:
        threshold = rf_rate
    ds_vol = oneside_vol(nav, bnav, freq, method, threshold, -1)[0]
    return (cpr_ret / ds_vol, )


def max_gl(nav, bnav, method='plain', window=20, gl_flag=1, excess_ret_flag=False):
    '''
    滚动窗口的最大收益或损失

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    method: string, default 'plain'
        收益计算方式，可选有[log, plain]
    window: pandas.Series
        收益或损失的计算窗口长度
    gl_flag: int, default 1
        标记计算收益还是损失，1表示收益，-1表示损失
    er_flag: boolean, default False
        是否以超额数据为基础计算

    Return
    ------
    gl: (gl, gl_start, gl_end)
    '''
    validation_checker([1, -1])(gl_flag)
    ret = nav2ret(nav, method, window)
    if excess_ret_flag:
        ret = ret - nav2ret(bnav, method, window)
    mgl = gl_flag * np.max(ret * gl_flag)
    mgl_end = (ret * gl_flag).argmax()
    mgl_start = ret.index[ret.index.get_loc(mgl_end) - window]
    return (mgl, mgl_start, mgl_end)


def ret_statistics(nav, bnav, func, method='plain'):
    '''
    单位时间收益的统计量

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准收益
    func: function(pandas.Series)->tuple
        计算统计量的函数
    method: string, default 'plain'
        计算收益的方式，可选为[plain, log]

    Return
    ------
    stats: tuple
    '''
    ret = nav2ret(nav, method)
    return func(ret)


def period_ret(nav, bnav, period_identifier, method='plain', excess_ret_flag=False):
    '''
    分期间收益计算函数

    Parameter
    ---------
    nav: pandas.Series
        策略净值
    bnav: pandas.Series
        基准净值
    period_identifier: string
        对时间区间进行识别的字符串，例如'%Y'表示以年为单位，'%Y-%m'表示以月为单位
    method: string, default 'plain'
        收益计算方法，可选为[log, plain]，如果选择log，不推荐将excess_ret_flag设为True
    excess_ret_flag: boolean, default False
        是否计算超额收益

    Return
    ------
    rets: (pandas.Series, )
    '''
    def cal_ret(v):
        v = normalize_nav(v)
        period_end_v = v.groupby(lambda x: x.strftime(period_identifier)).tail(1)
        period_end_r = nav2ret(period_end_v, method)
        if method == 'plain':
            period_end_r.iloc[0] = period_end_v.iloc[0] - 1
        else:
            period_end_r.iloc[0] = np.log(period_end_v.iloc[0])
        return period_end_r
    period_end_ret = cal_ret(nav)
    if excess_ret_flag:
        period_end_bret = cal_ret(bnav)
        period_end_ret = period_end_ret - period_end_bret
    return (period_end_ret, )


# --------------------------------------------------------------------------------------------------
# 默认指标计算配置
DEFAULT_IAE_CONFIG = {'total_return': Indicator(total_return),
                      'annualized_return': Indicator(comparable_return),
                      'annualized_vol': Indicator(comparable_vol),
                      'max_drawndown': Indicator(max_drawndown),
                      'max_drawndown_duration': Indicator(max_drawndown_duration),
                      'rolling_drawndown': Indicator(rolling_drawndown),
                      'rolling_annualized_return': Indicator(rolling_comparable_return),
                      'win_rate': Indicator(win_rate),
                      'alpha': Indicator(raw_alpha),
                      'beta': Indicator(raw_beta),
                      'info_ratio': Indicator(info_ratio),
                      'sharp_ratio': Indicator(sharp_ratio),
                      'downside_vol': Indicator(partial(oneside_vol, direction=-1)),
                      'upside_vol': Indicator(partial(oneside_vol, direction=1)),
                      'sortino_ratio': Indicator(sortino_ratio),
                      'max_rolling_gain': Indicator(partial(max_gl, gl_flag=1)),
                      'max_rolling_loss': Indicator(partial(max_gl, gl_flag=-1)),
                      'return_skew': Indicator(partial(ret_statistics, func=lambda x: (sp_stats.skew(x), ))),
                      'return_kurtosis': Indicator(partial(ret_statistics, func=lambda x: (sp_stats.kurtosis(x), ))),
                      'yearly_return': Indicator(partial(period_ret, period_identifier='%Y')),
                      'monthly_return': Indicator(partial(period_ret, period_identifier='%Y-%m'))}
general_iae_factory = IAEFactory(DEFAULT_IAE_CONFIG)