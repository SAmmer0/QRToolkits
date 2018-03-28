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

from qrtutils import parse_config, trans_config_sep
from qrtconst import Frequency, REL_PATH_HEADER, REL_PATH_HEADER_LEN
from tdtools.utils import set_logger

# 周期标记
class TargetSign(enum.Enum):
    LAST = enum.auto()
    FIRST = enum.auto()

# 模块文件夹
MODULE_PATH = dirname(__file__)

# 配置文件路径，可为相对路径和绝对路径，路径格式为./config.json或者C:\\Users\\default\\config.json
CONFIG_PATH = './config.json'
# 将路径分隔符转换为系统分隔符，并将相对路径转换为绝对路径
CONFIG_PATH = trans_config_sep(CONFIG_PATH)
if CONFIG_PATH.startswith(REL_PATH_HEADER):
    CONFIG_PATH = join(MODULE_PATH, CONFIG_PATH[REL_PATH_HEADER_LEN:])

# 加载配置文件
CONFIG = parse_config(CONFIG_PATH)
# 设置日志
LOGGER_NAME = set_logger(CONFIG['log'], MODULE_PATH)

# 交易所开市收市时间设置
TRADING_TIME = {'stock.sse': (('09:30', '11:30'), ('13:00', '15:00'))}
