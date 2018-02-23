# -*- encoding: utf-8

from os.path import dirname
import enum

from database.utils import submodule_initialization, set_logger
from database.const import DataFormatCategory, NaS, FilledStatus

SUBMODULE_NAME = 'json5db'
# 设置日志选项
DB_CONFIG = submodule_initialization(SUBMODULE_NAME, dirname(__file__))

LOGGER_NAME = set_logger(DB_CONFIG['log'])

# 文件后缀
SUFFIX = '.json'

