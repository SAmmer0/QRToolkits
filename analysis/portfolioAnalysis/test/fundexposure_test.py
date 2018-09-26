#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-09-26 10:41:00
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

import pdb

from analysis.portfolioAnalysis.analysorcore import ExposureAnalysor
import fmanager
from datasource.sqlserver.jydb import jydb
from datasource.sqlserver.utils import fetch_db_data
from datatoolkits import add_suffix
from datautils import DataView
from tdtools import get_calendar


def get_fund_detail_position(fund_symbol, rpt_date):
    """
    从数据库中获取给定基金的持仓

    Parameter
    ---------
    fund_symbol: string
        基金的代码，可以通过各大基金网站查询
    rpt_date: datetime like
        基金报告期

    Return
    ------
    position: pandas.Series
    """
    sql = '''
        SELECT M2.SecuCode, S.RatioInNV
        FROM SecuMain M, MF_StockPortfolioDetail S, SecuMain M2
        WHERE
            S.InnerCode = M.InnerCode AND
            M.SecuCode = \'{fund_symbol}\' AND
            S.StockInnerCode = M2.InnerCode AND
            M.SecuCategory = 8 AND
            M2.SecuCategory = 1 AND
            M2.SecuMarket in (83, 90) AND
            S.ReportDate = \'{rpt_date}\'
        '''.format(fund_symbol=fund_symbol, rpt_date=rpt_date)

    fund_data = fetch_db_data(jydb, sql, ['symbol', 'ratio'],
                              dtypes={'ratio': 'float64'})
    fund_data.symbol = fund_data.symbol.apply(add_suffix)
    fund_data = fund_data.set_index('symbol')['ratio']
    return fund_data


def analysis_exposure(port, benchmark_port, date):
    """
    分析组合相对于基准的暴露情况

    Parameter
    ---------
    port: pandas.Series
        组合的持仓，index为股票代码，值为权重
    benchmark_port: pandas.Series
        基准的持仓，格式同上，None表示没有基准
    date: datetime like
        计算暴露的时间

    Return
    ------
    exposure: pandas.Series
    """
    barra_vsf = fmanager.query('BARRA_VSF', date).iloc[0]
    mask = barra_vsf == 1
    port = port.loc[mask]
    if benchmark_port is not None:
        benchmark_port = benchmark_port.loc[mask]
        benchmark_port = benchmark_port / benchmark_port.sum()
    factors = [f for f in fmanager.list_allfactor()
               if f.startswith('BARRA_RF')]
    factors += ['ZX_IND']
    analysor = ExposureAnalysor({f: DataView(fmanager.generate_getter(f),
                                             get_calendar('stock.sse')) for f in factors},
                                'ZX_IND')
    exposure = analysor.calculate_exposure(date, port.to_dict(),
                                           benchmark_port.to_dict(), True)
    return exposure


def main_test(fund_symbol, benchmark_name):
    rpt_date = '2018-06-30'
    date = get_calendar('stock.sse').latest_tradingday(rpt_date, 'FUTURE')
    benchmark_weights = fmanager.query(benchmark_name, date).iloc[0].dropna()
    fund_port = get_fund_detail_position(fund_symbol, rpt_date)
    exposure = analysis_exposure(fund_port, benchmark_weights, date).drop('NaS')
    return exposure


if __name__ == '__main__':
    # res = main_test('161607', 'CNI100_WEIGHTS')
    res = main_test('110003', 'IH_WEIGHTS')