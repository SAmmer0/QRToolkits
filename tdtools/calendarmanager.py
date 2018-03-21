#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/21

负责所有日历的管理工作，包括添加新的日历实例、对日历的数据进行存取和更新
"""
from timeutils.const import CONFIG, STOCK_TRADING_PERIOD
from timeutils.tradingcalendar import TradingCalendar
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

def create_calendar(rel_path, trading_time):
    '''
    用于创建各个交易所或者市场的交易日历实例，要求在创建前日历的数据已经被存储在指定的相对路径下
    Parameter
    ---------
    rel_path: string
        交易日历存储的相对路径
    trading_time: tuple
        交易时间段设置

    Return
    ------
    out: TradingCalendar
    '''
    data = calendar_db.query(rel_path, (DataClassification.UNSTRUCTURED, ))
    obj = TradingCalendar(data, trading_time)
    return obj

# --------------------------------------------------------------------------------------------------
# 交易日历实例
sse_calendar = create_calendar('stock.sse', STOCK_TRADING_PERIOD)
