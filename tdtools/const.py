#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/16
"""

import enum
from os.path import join, dirname

from qrtutils import parse_config
from qrtconst import Frequency
from timeutils.utils import set_logger

# 周期标记
class TargetSign(enum.Enum):
    LAST = enum.auto()
    FIRST = enum.auto()


# 加载配置文件
CONFIG = parse_config(join(dirname(__file__), 'config.json'))

# 设置日志
LOGGER_NAME = set_logger(CONFIG['log'])

# 股票市场开市收市时间设置
STOCK_TRADING_PERIOD = (('09:30', '11:30'), ('13:00', '15:00'))
