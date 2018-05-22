#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/17
"""
import enum
from os.path import dirname, join
from os import sep, makedirs
import logging

from database import DataClassification, DataFormatCategory, DataValueCategory
from qrtutils import parse_config, set_logger

# --------------------------------------------------------------------------------------------------
# 数据更新日志设置函数
def set_updating_logger(cfg, main_logger_name):
    '''
    对数据更新日志进行设置，当前设置仅添加日志写入到文件的功能，如果需要打印或者其他输出，需要自行添加，使用完后，
    推荐将设置恢复到原始设置

    Parameter
    ---------
    cfg: dict
        模块配置

    Return
    ------
    logger_name: string
    '''
    db_path = cfg['db_path']
    logger_name = main_logger_name + '.' + db_path.split(sep)[-1]    # 日志名称为: 主logger名称.数据库名称
    logger = logging.getLogger(logger_name)
    if logger.handlers:    # 避免重复设置日志处理函数
        return logger.name
    formater = logging.Formatter("%(asctime)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.setLevel(logging.DEBUG)
    try:
        handler = logging.FileHandler(join(db_path, 'data_updating.log'))
    except FileNotFoundError:
        makedirs(db_path)
        handler = logging.FileHandler(join(db_path, 'data_updating.log'))
    handler.setFormatter(formater)
    logger.addHandler(handler)
    return logger.name

# --------------------------------------------------------------------------------------------------
# 常量
# 数据格式定义和映射
class DataType(enum.Enum):
    PANEL_NUMERIC = enum.auto()  # 数值型面板数据
    PANEL_CHAR = enum.auto()    # 字符型面板数据
    TS_NUMERIC = enum.auto()    # 数值型时间序列数据
    TS_CHAR = enum.auto()   # 字符型时间序列数据

DT_MAP = {
    DataType.PANEL_NUMERIC: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                             'dtype': 'float64'},
    DataType.PANEL_CHAR: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL),
                          'dtype': None},
    DataType.TS_NUMERIC: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES),
                          'dtype': 'float64'},
    DataType.TS_CHAR: {'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES),
                       'dtype': None}
}

# 加载和设置配置
CONFIG = parse_config(join(dirname(__file__), 'config.json'))

# 设置日志
LOGGER_NAME = set_logger(CONFIG['log'], dirname(__file__), 'PITDATA')
UPDATING_LOGGER =  set_updating_logger(CONFIG, LOGGER_NAME)

# 元数据文件名
METADATA_FILENAME = '#update_time_metadata.json'

# 更新的隔断时间(24小时制的小时时间)。即若当天时间早于该时间，以上个交易日为最新的时间
UPDATE_TIME_THRESHOLD = 18

# 计算文件所在的文件夹
CALCULATION_FOLDER_PATH = CONFIG['data_description_file_path']

# 配置文件中设置的数据开始时间
DATA_START_DATE = CONFIG['data_start_date']
