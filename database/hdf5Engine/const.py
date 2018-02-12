# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 16:54:58 2018

@author: Hao Li
"""


from os.path import dirname
import enum

from database.utils import submodule_initialization, set_logger
from database.const import DataFormatCategory, NaS

SUBMODULE_NAME = 'hdf5db'
# 设置日志选项
DB_CONFIG = submodule_initialization(SUBMODULE_NAME, dirname(__file__))

LOGGER_NAME = set_logger(DB_CONFIG['log'])

# 文件后缀
SUFFIX = '.h5'

# 数据文件填充状态


class FilledStatus(enum.Enum):
    FILLED = enum.auto()
    EMPTY = enum.auto()
