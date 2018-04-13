#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/9
"""
import pandas as pd
import numpy as np

from tdtools import get_calendar, trans_date, get_last_rpd_date, generate_rpd_series
from datasource.sqlserver.utils import transform_data, expand_data
from datasource.sqlserver.jydb.dbengine import jydb


def map2td(data, days, timecol=None, from_now_on=True, fillna=None):
    '''
    将(一系列)数据映射到给定的交易日

    Parameter
    ---------
    data: pandas.DataFrame
        待映射的数据，数据中必须包含时间特征(在数据列中或者在index中)
    days: iterable
        元素为需要映射到的目标日序列
    timecol: string, default None
        若数据中包含时间列，则需要提供时间列的列名；该参数为None表示时间包含在index中
    from_now_on: boolean
        映射规则。若from_now_on为True，表示给定时点及其后的值等于该时点的值，直至下一个时点；
        若该参数为False，表示仅该时点之后的值等于该时点的值，直至下一个时点。
    fillna: function, default None
        默认为None表示不对缺省数据做填充；若需要填充，对应的格式为{col: function(data)->value}，
        即填充函数接受原数据作为输入，返回一个填充的值

    Return
    ------
    out: pandas.DataFrame
        映射后的数据，返回数据的格式与传入数据相同(即，若index为时间，返回值中index也为时间)
    '''
    days = sorted(days)
    if timecol is not None:
        data = data.set_index(timecol, drop=True)
    mapped_tds = sorted([t for t in data.index if t < days[0]]) + days
    out = data.reindex(mapped_tds, method='ffill')
    if not from_now_on:
        out = out.shift(1)
    out = out.reindex(days)
    if timecol is not None:
        out = out.reset_index()
    if fillna:
        for col in fillna:
            func = fillna[col]
            fill_value = func(data)
            out[col] = out[col].fillna(fill_value)
    return out


def get_jydb_tds(start_time, end_time, market_type=83):
    '''
    从聚源数据库中获取交易日数据

    Parameter
    ---------
    start_time: datetime like
        开始时间
    end_time: datetime like
        结束时间
    market_type: int, default 83
        市场类型，具体参照聚源数据库QT_TradingDayNew文档，默认83表示上交所和深交所

    Return
    ------
    out: pandas.Series
        结果包含首尾(若首尾为交易日)
    '''
    sql = '''
    SELECT TradingDate
    FROM QT_TradingDayNew
    WHERE
            IfTradingDay = 1 AND
            SecuMarket = {mt} AND
            TradingDate > \'{st:%Y-%m-%d}\' AND
            TradingDate < \'{et:%Y-%m-%d}\'
    '''
    start_time, end_time = trans_date(start_time, end_time)
    sql = sql.format(mt=market_type, st=start_time, et=end_time)
    data = jydb.fetchall(sql)
    out = transform_data(data, ['trading_days']).iloc[:, 0].tolist()
    return out


def process_fundamental_data(data, cols, start_time, end_time, max_hist_num, func, **kwargs):
    '''
    处理从数据库中取出的基本面数据，按照给定的函数计算基本面数据，并将基本面数据映射到交易日中

    Parameter
    ---------
    data: pandas.DataFrame
        原始数据，必须包含cols参数中的列名
    cols: iterable
        元素为列名，长度为4，依次表示的意思是[证券代码列, 数据列, 更新时间列, 报告期列]
    start_time: datetime like
        数据的开始时间
    end_time: datetime like
        数据的结束时间
    max_hist_num: int
        每个更新日期保存的历史最新数据的最大数量
    func: function
        格式为function(pandas.DataFrame)-> value，该函数接受的参数为每个股票在每个更新日期
        的最新历史数据，包含cols参数中的列
    kwargs: dict
        以字典形式传给function的参数

    Return
    ------
    out: pandas.DataFrame
        处理后生成的二维表，index为时间列，columns为股票代码列
    '''
    start_time, end_time = trans_date(start_time, end_time)
    symbol_col, data_col, ut_col, rpt_col = cols
    data = data.sort_values([symbol_col, ut_col])
    tds = get_calendar('stock.sse').get_tradingdays(start_time, end_time)
    def handler_per_symbol(df):
        # 对单只证券进行处理
        obs_data, obs_flag = expand_data(df, ut_col, rpt_col, max_hist_num)
        res = obs_data.groupby(obs_flag).apply(func, **kwargs)
        res = map2td(res, tds).reset_index().rename(columns={obs_flag: 'time', 0: 'data'})
        return res
    out = data.groupby(symbol_col).apply(handler_per_symbol).reset_index()
    out = out.pivot_table('data', index='time', columns=symbol_col)
    return out

def calc_tnm(df, data_col, period_flag_col, nperiod=4):
    '''
    计算TTM的变种算法，用于计算最新的n个季度数据的和

    Parameter
    ---------
    df: pandas.DataFrame
        至少包含data_col和period_flag_col两列
    data_col: string
        数据所在的列列名
    period_flag_col: string
        报告期标记所在的列列名
    nperiod: int, default 4
        计算数据加和的期数

    Return
    ------
    result: float

    Notes
    -----
    nperiod参数应该大于1，可能会出现无法预测的异常，等于1时推荐使用calc_offsetdata
    '''
    if len(df) < nperiod:
        return np.nan
    last_rpt_date = get_last_rpd_date(df[period_flag_col])
    if last_rpt_date is None:
        return np.nan
    rpts = generate_rpd_series(last_rpt_date, nperiod)
    target_df = df.loc[df[period_flag_col].isin(rpts), data_col]
    if len(target_df) < nperiod:
        return np.nan
    if len(target_df) > nperiod:
        raise ValueError('Data length exceeds that of required!')
    return target_df.sum(skipna=False)


def calc_offsetdata(df, data_col, period_flag_col, offset, multiple):
    '''
    提取往前推移给定期数的基本面数据值，例如获取最近财年的数据或者最近季度的数据

    Parameter
    ---------
    df: pandas.DataFrame
        至少包含data_col和period_flag_col两列
    data_col: string
        数据所在列的列名
    period_flag_col: string
        报告期标记所在列的列名
    offset: int
        往前推移的期数，其中1表示往前最近的一期，2表示次前的一期，以此类推
        例如，当前日期为2018-03-30，数据中报告期依次为[2017-12-31, 2017-09-30, 2017-06-30]，
        以季度为例，1对应着2017-12-31，2对应着2017-09-30，依次类推
    multiple: int
        指查找给定数据的时间跨度，以季度为最基本的单位，当前仅支持[1(季度), 2(半年度), 4(半年度)]。

    Return
    ------
    result: float
    '''
    if multiple not in [1, 2, 4]:
        raise ValueError('Improper parameter \"multiple\",'+
                         ' valids are [1, 2, 4], you provide {}.'.format(multiple))
    if len(df) < offset:
        return np.nan
    rpt_col = df[period_flag_col]
    last_rpt_date = get_last_rpd_date(rpt_col)
    if last_rpt_date is None:
        return np.nan
    rpts = generate_rpd_series(last_rpt_date, offset*multiple)
    multiple = 3 * multiple
    target = [t for t in rpts if t.month % multiple == 0][offset - 1]
    df = df.set_index(period_flag_col)
    try:
        result = df.loc[target, data_col]
    except KeyError:
        result = np.nan
    return result
