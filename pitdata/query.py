#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/18

用于处理主要的数据请求
"""
import logging
import re
import pdb

import pandas as pd

from pitdata.io import query_data, get_db_dictionary, show_db_structure
from pitdata.const import LOGGER_NAME

# --------------------------------------------------------------------------------------------------
# 预处理
# logger初始化
logger = logging.getLogger(LOGGER_NAME)

# --------------------------------------------------------------------------------------------------
# 功能函数
def query(data_name, start_time, end_time=None):
    '''
    单个数据的请求函数

    Parameter
    ---------
    data_name: string
        数据名称
    start_time: datetime like
        请求的起始时间
    end_time: datetime like, default None
        请求的终止时间，None表示请求横截面数据，仅面板数据才能请求

    Return
    ------
    out: pandas.DataFrame, pandas.Series or None
        None表示没有所请求的数据
    '''
    dmsg = get_db_dictionary().get(data_name, None)
    if dmsg is None:
        raise ValueError('Unrecognizable data name(name={})!'.format(data_name))
    out = query_data(dmsg['rel_path'], dmsg['datatype'], start_time, end_time)
    return out

def query_group(name_group, start_time, end_time=None):
    '''
    同时请求多个数据，并将该数据合成到一个pandas.DataFrame中，要求这些同时请求的数据的
    shape要相同，且具有相同的列名

    Parameter
    ---------
    name_group: list
        元素为数据名称
    start_time: datetime like
        起始时间
    end_time: datetime like, default None
        终止时间，None表示请求横截面数据，仅面板数据才能请求

    Return
    ------
    out: pandas.DataFrame or None
        若请求的是横截面数据，则index为数据名称
        若请求的是面板数据，则index为多重索引，0级是时间，1级是数据名称
        None表示请求的数据中至少有一个没有查询到给定要求的数据
    '''
    if len(name_group) <= 1:
        logger.warning('[Operation=query_group, '+
                       'Info=\"Parameter name_group has a length of {}, the result will be uncertain!\"]'.format(len(name_group)))
    datas = [query(d, start_time, end_time) for d in name_group]
    if any([x is None for x in datas]):
        return None
    data_shapes = [d.shape for d in datas]
    if not all(ldf == data_shapes[0] for ldf in data_shapes):
        raise ValueError('Input data should have the same shape!')
    if end_time is None:
        out = pd.concat(datas, axis=1)
        out.columns = name_group
        return out.T
    else:
        for d_name, d in zip(name_group, datas):
            d.index = pd.MultiIndex.from_product([d.index, [d_name]])
        out = pd.concat(datas, axis=0).sort_index(level=[0, 1])
        return out

def list_data(pattern=None, match_method='simple'):
    '''
    列出当前数据库中的数据名称

    Parameter
    ---------
    pattern: string, default None
        过滤模式，None表示不过滤
    match_method: string, default 'simple'
        过滤方法，支持['simple'(返回包含pattern参数中字符串的数据名称), 're'(返回匹配pattern中re表达式的数据名称)]

    Return
    ------
    out: list
    '''
    out = sorted(get_db_dictionary().keys())
    if pattern is not None:
        if match_method == 'simple':
            out = [n for n in out if pattern in n]
        elif match_method == 're':
            out = [n for n in out if re.match(pattern, n) is not None]
        else:
            raise ValueError('Valid match_method are [simple, re]!')
    return out

def show_all_data():
    '''
    打印整个数据库的组织结构
    '''
    show_db_structure()
