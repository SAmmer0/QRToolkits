# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 16:21:55 2018

@author: Hao Li

数据库模块一些辅助工具
"""
import logging
from os import sep
from sys import stdout

import qrtutils
from database.const import CONFIG_PATH, MODULE_NAME
from qrtconst import REL_PATH_HEADER

def submodule_initialization(db_name, path):
    '''
    对子模块根据配置进行初始化设置
    
    Parameter
    ---------
    db_name: string
        数据库子模块名称，必须与配置文件对应
    path: string
        调用模块所在文件夹的路径，通常为dirname(__file__)，用于将相对路径转化为绝对路径，
        此处假设子模块文件夹的母文件夹为主模块文件夹
    
    Return
    ------
    db_config: dict
        数据库子模块的配置
    '''
    config = qrtutils.parse_config(CONFIG_PATH)
    db_config = config[db_name]
    path = qrtutils.trans_config_sep(path)
    if not db_config['log']['enable_log']:  # 当前未启用单独记录日志，使用系统日志设置
        db_config['log'] = config['log']
        # 添加当前日志名
        db_config['log']['name'] = MODULE_NAME
        log_path = db_config['log']['log_path']
        if log_path.startswith(REL_PATH_HEADER):  # 相对路径转绝对路径
            module_path = sep.join(path.split(sep)[:-1])
            log_path = qrtutils.relpath2abs(module_path, log_path)
    else:
        db_config['log']['name'] = MODULE_NAME + '.' + db_name
        log_path = db_config['log']['log_path']
        if log_path.startswith(REL_PATH_HEADER):
            log_path = qrtutils.relpath2abs(path, log_path)
    db_config['log']['log_path'] = log_path
        
    return db_config

def set_logger(log_config):
    '''
    在子模块初始化时将对应的模块设置应用到系统中
    
    Parameter
    ---------
    log_config: dict
        日志配置
    
    Return
    ------
    logger_name: string
        logger的名称
    '''
    if not log_config['enable_log']:
        return None
    logger = logging.getLogger(log_config['name'])
    if logger.hasHandlers():    # 每个日志对象只有一个处理函数
        return logger.name
    logger.setLevel(getattr(logging, log_config['log_level']))
    formatter = logging.Formatter(log_config['format'], 
                                  log_config['date_format'])
    if log_config['log_to_file']:
        handler = logging.FileHandler(log_config['log_path'])
        handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler(stdout)
        handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger.name
    
    
