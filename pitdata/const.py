#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/17
"""
import enum
from os.path import dirname, join

from database import DataClassification, DataFormatCategory, DataValueCategory
from qrtutils import parse_config


# 数据格式定义和映射
class DataType(enum.Enum):
    PANEL_NUMERIC = enum.auto()  # 数值型面板数据
    PANEL_CHAR = enum.auto()    # 字符型面板数据
    TS_NUMERIC = enum.auto()    # 数值型时间序列数据
    TS_CHAR = enum.auto()   # 字符型时间序列数据

DT_MAP = {
    DataType.PANEL_NUMERIC: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                             'dtype': 'float64'},
    DataType.PANEL_CHAR: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL),
                          'dtype': None},
    DataType.TS_NUMERIC: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES),
                          'dtype': 'float64'},
    DataType.TS_CHAR: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES),
                       'dtype': None}
}

# 加载和设置配置
CONFIG = parse_config(join(dirname(__file__), 'config.json'))

# 元数据文件名
METADATA_FILENAME = '#update_time_metadata.json'

# 更新的隔断时间(24小时制的小时时间)。即若当天时间早于该时间，以上个交易日为最新的时间
UPDATE_TIME_THRESHOLD = 16
