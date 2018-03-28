#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/19
"""
import logging
from sys import stdout

from qrtconst import REL_PATH_HEADER
from qrtutils import relpath2abs


# --------------------------------------------------------------------------------------------------
# 功能函数
def set_logger(log_config, module_path):
    '''
    根据日志配置文件对日志选项进行设置

    Parameter
    ---------
    log_config: dict
        日志配置字典

    Return
    ------
    logger_name: string
    '''
    logger = logging.getLogger('TIME_UTILS')
    if logger.handlers:  # 每个logger只能有一个handler
        return logger.name
    formater = logging.Formatter(log_config['format'], log_config['date_format'])
    logger.setLevel(getattr(logging, log_config['log_level']))
    if log_config['log_to_file']:
        if log_config['log_path'].startswith(REL_PATH_HEADER):
            log_config['log_path'] = relpath2abs(module_path, log_config['log_path'])
        handler = logging.FileHandler(log_config['log_path'])
        handler.setFormatter(formater)
    else:
        handler = logging.StreamHandler(stdout)
        handler.setFormatter(formater)
    logger.addHandler(handler)
    return logger.name

