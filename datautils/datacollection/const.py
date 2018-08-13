#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/13
"""
from os.path import dirname
from qrtutils import set_logger

# 设置日志
LOG_CONFIG = {
        "log_to_file": True,
        "log_path": "./datacollection.log",
        "log_level": "DEBUG",
        "format": "%(asctime)s %(levelname)s %(filename)s-%(lineno)s: %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S"
}
LOGGER_NAME = 'DATA_COLLECTION'
set_logger(LOG_CONFIG, dirname(__file__), LOGGER_NAME)
