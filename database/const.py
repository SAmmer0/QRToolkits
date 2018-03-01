# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 15:18:54 2018

@author: Hao Li
"""

from os.path import dirname, join
import enum

from qrtconst import REL_PATH_HEADER, REL_PATH_HEADER_LEN, NaS, ENCODING
from qrtutils import trans_config_sep

CONFIG_PATH = './config.json'
# 将路径分隔符转换为系统分隔符，并将相对路径转换为绝对路径
CONFIG_PATH = trans_config_sep(CONFIG_PATH)
if CONFIG_PATH.startswith(REL_PATH_HEADER):
    CONFIG_PATH = join(dirname(__file__), CONFIG_PATH[REL_PATH_HEADER_LEN:])

# 设置模块名称，主要用于日志记录中
MODULE_NAME = 'qrt_database'

# 数据文件填充状态

class FilledStatus(enum.Enum):
    FILLED = enum.auto()
    EMPTY = enum.auto()

# 数据形式分类: 时间序列数据、面板数据


class DataFormatCategory(enum.Enum):
    PANEL = enum.auto()
    TIME_SERIES = enum.auto()

# 数值类型分类: 数字型、字符型
class DataValueCategory(enum.Enum):
    NUMERIC = enum.auto()
    CHAR = enum.auto()

# 数据分类: 结构化数据、非结构化数据
class DataClassification(enum.Enum):
    STRUCTURED = enum.auto()
    UNSTRUCTURED = enum.auto()
