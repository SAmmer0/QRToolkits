#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/21

负责所有日历的管理工作，包括添加新的日历实例、对日历的数据进行存取和更新
"""
from tdtools.const import CONFIG, TRADING_TIME
from tdtools.tradingcalendar import TradingCalendar
from database import Database, DataClassification

# --------------------------------------------------------------------------------------------------
# 全局变量设置
calendar_db = Database(CONFIG['calendar']['calendar_db_path'])

# --------------------------------------------------------------------------------------------------
# 处理函数
def update_data(data, rel_path):
    '''
    更新交易日历的数据
    Parameter
    ---------
    data: list like
        列表类型规范的日历数据
    rel_path: string
        日历数据的相对路径，以'.'作为层级分隔符，例如'stock.sse', 'future.shfe'

    Return
    ------
    result: boolean
    '''
    return calendar_db.insert(data, rel_path, (DataClassification.UNSTRUCTURED, ))

def get_calendar(calendar_rel_path):
    '''
    获取给定相对路径的日历，若没有对应的数据(日历数据或者交易时间数据)则报错
    若需要添加日历数据，调用update_data，若需要添加交易时间数据，需要手动添加到tdtools.const的TRADING_TIME中
    Parameter
    ---------
    calendar_rel_path: string
        日历的相对路径

    Return
    ------
    calendar: TradingCalendar
    '''
    if calendar_rel_path not in calendar_db.list_alldata() or calendar_rel_path not in TRADING_TIME:
        raise ValueError('data(path={}) cannot be found!'.format(calendar_rel_path))
    data = calendar_db.query(calendar_rel_path, (DataClassification.UNSTRUCTURED, ))
    obj = TradingCalendar(data, TRADING_TIME[calendar_rel_path])
    return obj
