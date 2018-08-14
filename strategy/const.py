#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/14
"""
from os.path import dirname
from qrtutils import set_logger

LOGGER_NAME = 'STRATEGY'

LOG_CONFIG = {
        "log_to_file": True,
        "log_path": "./strategy.log",
        "log_level": "DEBUG",
        "format": "%(asctime)s %(levelname)s %(filename)s-%(lineno)s: %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S"
}

set_logger(LOG_CONFIG, dirname(__file__), LOGGER_NAME)
