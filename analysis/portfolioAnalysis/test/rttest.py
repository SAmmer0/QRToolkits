#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/30
"""
import pandas as pd
import numpy as np

from fmanager import generate_getter
from datatoolkits import add_suffix
from datautils import DataView
from tdtools import get_calendar, timeit_wrapper
from analysis.portfolioAnalysis.analysorcore import ExposureCalculator

pos_path = r'E:\工作日志\2018\3月\resources\组合持仓记录.xlsx'
date = '2018-03-12'

pos_data = pd.read_excel(pos_path, pd.to_datetime(date).strftime('%Y%m%d'), dtype={'symbol': np.unicode}, usecols=[4, 7])
pos_data.loc[:, 'symbol'] = pos_data.symbol.apply(add_suffix)
pos_data = pos_data.set_index('symbol')['position'].to_dict()

def generate_dv(factor_name):
    return DataView(generate_getter(factor_name), get_calendar('stock.sse'))


factors = ['BARRA_BETA', 'BARRA_LNCAP', 'BARRA_BTOP', 'BARRA_CMRA', 'BARRA_DASTD', 'BARRA_HSIGMA', 'ZX_IND']
factor_datas = {fn: generate_dv(fn) for fn in factors}

xcalculator = ExposureCalculator(factor_datas, industry_fn='ZX_IND')
# 速度比较慢可能是由于行业数据加载速度慢导致的
xcal = timeit_wrapper(xcalculator.calculate_exposure)
tmp = xcal(date, pos_data)
