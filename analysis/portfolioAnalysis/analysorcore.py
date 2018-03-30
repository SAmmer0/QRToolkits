#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/29
"""
import pandas as pd

from qrtconst import CASH

class ExposureCalculator(object):
    '''
    用于计算因子暴露的类
    Parameter
    ---------
    factor_data: dict
        元素为{factor_name: object}，对object的要求是能够通过get_csdata(date)函数获取给定日期的因子暴露，
        当前object应该为datautils.DataView或者其子类
    industry_fn: string, default None
        factor_data中若包含了股票所属行业的数据(行业数据为字符串类型，在使用前需要转换为数值型dummy数据)，
        默认为None表示不包含行业数据
    '''
    def __init__(self, factor_data, industry_fn=None):
        self._factor_data = factor_data    # 此处使用的是浅复制，具有一定的风险
        self._industry_fn = industry_fn

    def add_factor(self, factor_name, factor_data):
        '''
        添加新的因子数据
        Parameter
        ---------
        factor_name: string
            添加的因子数据的名称，要求不能与已经有的因子数据名称重复，重复会导致报错(ValueError)
        factor_data: object
            因子数据，必须有get_csdata(date)方法，推荐datautils.DataView或者其子类
        '''
        if factor_name in self._factor_data:
            raise ValueError('Duplicate factor name! {} is already contained!'.format(factor_name))
        self._factor_data[factor_data] = factor_data

    def delete_factor(self, factor_name):
        '''
        删除给定的因子数据
        Parameter
        ---------
        factor_name: string
            需要删除的因子数据名称，若该名称的数据不存在将报错(ValueError)
        '''
        if factor_name not in self._factor_data:
            raise ValueError('Factor data({}) cannot be found!'.format(factor_name))
        del self._factor_data[factor_name]

    @staticmethod
    def _handle_cash(data):
        '''
        大多数因子数据中没有现金项，需要额外添加，此处的处理方式仅为将现金项的所有因子暴露(NA值)设置为0
        Parameter
        ---------
        data: pandas.DataFrame
            因子数据，index为证券代码，columns为因子数据

        Return
        ------
        out: pandas.DataFrame
            数据的index部分添加CASH项，并且填充了相应的数据
        '''
        if CASH not in data.index:
            new_ind = data.index.union([CASH])
            data = data.reindex(index=new_ind)
        else:
            data = data.copy()
        data.loc[CASH, :] = data.loc[CASH].fillna(0)
        return data

    @staticmethod
    def _handle_industry(data):
        '''
        将字符串类型的行业数据转换为对应的dummy数据
        Parameter
        ---------
        data: pandas.Series
            行业分布数据，index为证券代码，值为每个证券所属的行业字符串

        Return
        ------
        out: pandas.DataFrame
        '''
        return pd.get_dummies(data)

    def _combine_datas(self, date):
        '''
        通过内置缓存获取数据，并对数据进行预处理整合
        Parameter
        ---------
        date: datetime
            获取数据的时间

        Return
        ------
        out: pandas.DataFrame
        '''
        datas = self._factor_data
        raw_data = {fn: datas[fn].get_csdata(date) for fn in datas}
        if self._industry_fn is not None:
            ind_data = self._handle_industry(raw_data[self._industry_fn])
            del raw_data[self._industry_fn]
            raw_data = pd.DataFrame(raw_data)
            raw_data = pd.concat([raw_data, ind_data], axis=1)
        else:
            raw_data = pd.DataFrame(raw_data)
        raw_data = self._handle_cash(raw_data)
        return raw_data

    def calculate_exposure(self, date, portfolio, benchmark=None):
        '''
        计算给定组合在特定日期的因子暴露
        Parameter
        ---------
        date: datetime like
            计算因子暴露的日期
        portfolio: dict
            格式为{symbol: weight}，若包含现金，现金的标识为"CASH"(或者qrtconst.CASH)，如果不包含
            现金，则会自行根据当前持仓中的权重计算现金的占比
        benchmark: dict, default None
            基准组合，格式与portfolio相同，若基准中没有现金，会做同样的处理。若该参数为None，则默认基准为
            100%的现金

        Return
        ------
        exposure: pandas.Series
            index为因子名称，值为相关暴露
        '''
        def add_cash(port):
            if CASH not in port:
                cash_weight = 1 - sum(port.values())
                if cash_weight < 0:
                    raise ValueError('The sum of portfolio weights exceeds 1!')
                port[CASH] = cash_weight
            return pd.Series(port)

        portfolio = add_cash(portfolio)
        if benchmark is None:
            benchmark = pd.Series({sn: 0 if sn != CASH else 1 for sn in portfolio.index})
        else:
            benchmark = add_cash(benchmark)
        idx = portfolio.index.union(benchmark.index)
        factor_data = self._combine_datas(date).reindex(idx)
        exceeded_port = portfolio.reindex(idx).fillna(0) - benchmark.reindex(idx).fillna(0)
        exposure = exceeded_port.dot(factor_data)
        return exposure
