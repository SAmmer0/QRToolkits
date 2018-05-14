#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/17
"""
from os.path import split as op_split

import pandas as pd

from pitdata.const import CONFIG, DT_MAP
from database import Database
from database.const import REL_PATH_SEP

# --------------------------------------------------------------------------------------------------
# 加载数据库实例，一次性加载避免过多的元数据初始化
db = Database(CONFIG['db_path'])

# --------------------------------------------------------------------------------------------------
# IO功能函数
def insert_data(data, rel_path, datatype):
    '''
    向数据库中插入数据

    Parameter
    ---------
    data: pandas.Series or pandas.DataFrame
        需要插入的数据，具体类型与datatype参数有关
    rel_path: string
        待插入的数据的相对路径
    datatype: pitdata.const.DataType
        待插入数据的类型，若类型以TS开头，则data参数为pandas.Series类型，反之则应该为pandas.DataFrame类型

    Return
    ------
    result: boolean
    '''
    if ((isinstance(data, pd.DataFrame) and not datatype.name.startswith('PANEL_')) or
        (isinstance(data, pd.Series) and not datatype.name.startswith('TS_'))):
        raise ValueError('Improper parameter combination(data: {datat}, datatype: {dt})!'.
                         format(datat=type(data), dt=datatype.name))
    insert_param = DT_MAP[datatype]
    result = db.insert(data, rel_path, store_fmt=insert_param['store_fmt'], dtype=insert_param['dtype'])
    return result


def query_data(rel_path, datatype, start_time, end_time=None):
    '''
    从数据库中请求数据

    Parameter
    ---------
    rel_path: string
        请求数据的相对路径
    datatype: pitdata.const.DataType
        数据类型
    start_time: datetime like
        数据开始时间
    end_time: datetime like, default None
        数据结束时间。该参数为None表示请求横截面数据，仅当datatype为面板数据时，该参数才可能为None

    Return
    ------
    out: pandas.Series or pandas.DataFrame
    '''
    if datatype.name.startswith('TS_') and end_time == None:
        raise ValueError('Time series cannot query cross-section data!')
    query_param = DT_MAP[datatype]
    out = db.query(rel_path, query_param['store_fmt'], start_time, end_time)
    return out


def delete_data(rel_path, datatype):
    '''
    删除给定路径的数据

    Parameter
    ---------
    rel_path: string
        数据的路径
    datatype: pitdata.const.DataType
        数据类型

    Return
    ------
    result: boolean
    '''
    delete_param = DT_MAP[datatype]
    result = db.remove_data(rel_path, delete_param['store_fmt'])
    return result

def move_data(src_path, dest_path, datatype):
    '''
    将数据从一个位置移动到另一位置

    Parameter
    ---------
    src_path: string
        原文件所在的相对路径
    dest_path: string
        目标相对路径
    datatype: pitdata.const.DataType
        数据类型

    Return
    ------
    result: boolean

    Notes
    -----
    目标相对路径的有效性由调用程序做检查
    '''
    move_param = DT_MAP[datatype]
    result = db.move_to(src_path, dest_path, move_param['store_fmt'])
    return result

def get_db_dictionary():
    '''
    以字典的形式返回当前数据库中包含的所有数据

    Return
    ------
    out: dict
        格式为{data_name: {'rel_path': rel_path, 'datatype': datatype}}
    '''
    db_name = op_split(CONFIG['db_path'])[1]
    all_data_node = db.find_collection(db_name)['']
    reverse_map = {DT_MAP[k]['store_fmt']: k for k in DT_MAP}
    out = {d['rel_path'].split(REL_PATH_SEP)[-1]:
           {'rel_path': d['rel_path'], 'datatype': reverse_map[d['store_fmt'].data]}
           for d in all_data_node}
    return out

def show_db_structure():
    '''
    打印数据库的结构
    '''
    db.print_collections()
