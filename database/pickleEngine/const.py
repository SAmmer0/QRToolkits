#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/13
"""
from os.path import dirname

from database.utils import submodule_initialization, set_logger
from database.const import REL_PATH_SEP, ENCODING

SUBMODULE_NAME = 'pickledb'
# 设置日志选项
DB_CONFIG = submodule_initialization(SUBMODULE_NAME, dirname(__file__))

LOGGER_NAME = set_logger(DB_CONFIG['log'])

# 文件后缀
SUFFIX = '.pickle'

# pickle协议
PICKLE_PROTOCOL = 4
