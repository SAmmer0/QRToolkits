#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/4

包含SQL数据的基本处理函数
"""
import pyodbc
import time
import pdb

import pandas as pd

from tdtools import trans_date


# --------------------------------------------------------------------------------------------------
# 工具函数
def transform_data(data, colnames, dtypes=None):
    '''
    将数据库中获取的原始数据转换为常用(Pandas可解析)的数据类型

    Parameter
    ---------
    data: list
        元素为原始数据库中行数据(pyodbc.Row)
    colnames: list
        列名
    dtypes: dict, default None
        数据列到类型的映射，格式为{colname: dtype(string or numpy dtype)}，
        默认None表示不需要进行转换

    Return
    ------
    out: pandas.DataFrame
        若data中没有任何数据，则返回空pandas.DataFrame
    '''
    if not data:
        return pd.DataFrame(columns=colnames)
    data = pd.DataFrame([dict(zip(colnames, r)) for r in data])
    if dtypes is not None:
        data = data.astype(dtypes)
    return data


def fetch_db_data(db, sql, colnames, dtypes=None, sql_kwargs=None):
    '''
    从数据库中获取数据

    Parameter
    ---------
    db: Object
        数据库引擎实例对象，要求必须要有fetchall方法
    sql: string
        获取数据的SQL语句，可以是模板，需要替代的部分由format处理
    colnames: list
        数据的列名
    dtype: dict, default None
        数据类型转换的映射，格式为{colname: dtype(string or numpy dtype)}，默认
        为None表示不需要映射
    sql_kwargs: dict, default None
        若sql参数为SQL模板，则需要提供参数对应的映射，默认为None，表示
        未使用模板

    Return
    ------
    out: pandas.DataFrame
    '''
    if sql_kwargs is not None:
        sql = sql.format(**sql_kwargs)
    data = db.fetchall(sql)
    data = transform_data(data, colnames, dtypes)
    return data


def expand_data(data, time_col, period_flag_col):
    '''
    将数据进行扩展，计算每个时间点能够观测到的最新的历史数据

    Parameter
    ---------
    data: pandas.DataFrame
        原始数据，数据已经按照time_col进行升序排列
    time_col: string
        数据观测时间所在列列名，例如基本面数据的数据更新时间
    period_flag_col: string
        数据对应期数的标记所在列的列名，例如数据对应的报告期

    Return
    ------
    out: pandas.DataFrame
        在数据中额外添加一列'__TIME_FLAG__'用来标记当前数据对应的观察时间
    flag_col_name: string
        额外添加一列的列名
    '''
    flag = '__TIME_FLAG__'
    out = []
    for udt in data[time_col].unique():
        tmp_data = data[data[time_col] <= udt]     # 观测日的数据需要包含当天的
        by_rpt = tmp_data.groupby(period_flag_col, as_index=False)
        tmp_res = by_rpt.tail(1)
        tmp_res[flag] = udt
        out.append(tmp_res)
    out = pd.concat(out, axis=0)
    return out, flag

# --------------------------------------------------------------------------------------------------
# 类
class SQLConnector(object):
    '''
    对pyodbc模块进行包装

    Parameter
    ---------
    database: string
        数据库名称
    server: string
        服务器地址
    uid: string
        用户名
    pwd: string
        密码
    driver: string
        驱动
    '''
    def __init__(self, database, server, uid, pwd, driver):
        self._database = database
        self._server = server
        self._uid = uid
        self._pwd = pwd
        self._driver = driver
        self._conn = None

    def fetchall(self, sql, *args, **kwargs):
        '''
        从数据库中提取数据

        Parameter
        ---------
        sql: string
            获取数据的SQL语句
        args: iterable
            其他可迭代的SQL参数
        kwargs: dict
            其他键值类型的SQL参数

        Return
        ------
        out: list
            元素为pyodbc.Row
        '''
        self._conn = pyodbc.connect(DATABASE=self._database, SERVER=self._server, UID=self._uid,
                                   PWD=self._pwd, DRIVER=self._driver)
        try:
            cur = self._conn.cursor()
            cur.execute(sql, *args, **kwargs)
            out = cur.fetchall()
        finally:
            self._conn.close()
        return out
