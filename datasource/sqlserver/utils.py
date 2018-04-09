#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/4

包含SQL数据的基本处理函数
"""

import pandas as pd

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


def expand_data(data, time_col, period_flag_col, max_hist_num):
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
    max_hist_num: int
        每个观测时间点保留的最新数据的数量

    Return
    ------
    out: pandas.DataFrame
        在数据中额外添加一列'__TIME_FLAG__'用来标记当前数据对应的观察时间
    flag_col_name: string
        额外添加一列的列名
    '''
    flag = '__TIME_FLAG__'
    out = pd.DataFrame()
    for udt in data[time_col].unique():
        tmp_data = data[data[time_col] <= udt]     # 观测日的数据需要包含当天的
        by_rpt = tmp_data.groupby(period_flag_col, as_index=False)
        tmp_res = by_rpt.apply(lambda x: x.tail(1))
        tmp_res[flag] = udt
        out = out.append(tmp_res)
    out = out.groupby(flag, as_index=False).apply(lambda x: x.tail(max_hist_num))\
        .reset_index(drop=True)
    return out, flag

if __name__ == '__main__':
    from sysconfiglee import get_database
    from tdtools.tools import timeit_wrapper
    jydb = get_database('jydb')
    sql = '''
    SELECT S.InfoPublDate, S.EndDate, M.SecuCode, S.NPFromParentCompanyOwners
    FROM LC_QIncomeStatementNew S, SecuMain M
    WHERE
        M.CompanyCode = S.CompanyCode AND
        M.SecuMarket in (83, 90) AND
        M.SecuCategory = 1 AND
        S.BulletinType != 10 AND
        S.EndDate >= CAST(\'{start_time}\' AS datetime) AND
        S.InfoPublDate <= CAST(\'{end_time}\' AS datetime) AND
        S.EndDate >= (SELECT TOP(1) S2.CHANGEDATE
                      FROM LC_ListStatus S2
                      WHERE
                          S2.INNERCODE = M.INNERCODE AND
                          S2.ChangeType = 1)
    ORDER BY M.SecuCode ASC, S.InfoPublDate ASC
    '''
    fetch_db_data = timeit_wrapper(fetch_db_data)
    data = fetch_db_data(jydb, sql, ['update_time', 'rpt_date', 'code', 'data'], {'data': 'float64'},
                         {'start_time': '2016-01-01', 'end_time': '2018-04-01'})
    expand_data = timeit_wrapper(expand_data)
    sample_data = data.groupby('code').get_group('000001')
    res, flag = expand_data(sample_data, 'update_time', 'rpt_date', 3)
