# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 16:54:58 2018

@author: Hao Li
"""


from os.path import dirname

from database.utils import submodule_initialization, set_logger
from database.const import PANEL, TIME_SERIES

SUBMODULE_NAME = 'HDF5'
# 设置日志选项
DB_CONFIG = submodule_initialization(SUBMODULE_NAME, dirname(__file__))

LOGGER_NAME = set_logger(DB_CONFIG['log'])
