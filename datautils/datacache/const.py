#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/22
"""
import enum
from os.path import dirname

from qrtutils import set_logger

# 数据缓存状态
class CacheStatus(enum.Enum):
    FUTURE = enum.auto()
    PAST = enum.auto()
    BOTH = enum.auto()
    ENOUGH = enum.auto()

# 日志记录配置
LOG_CONFIG = {
        "log_to_file": True,
        "log_path": "./datachche.log",
        "log_level": "DEBUG",
        "format": "%(asctime)s %(levelname)s %(filename)s-%(lineno)s: %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S"
}
# Logger名称
LOGGER_NAME = 'DATA_CACHE'
# 设定日志配置
set_logger(LOG_CONFIG, dirname(__file__), LOGGER_NAME)
