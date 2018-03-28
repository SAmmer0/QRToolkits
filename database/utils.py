# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 16:21:55 2018

@author: Hao Li

数据库模块一些辅助工具
"""
import logging
from os import sep
from sys import stdout
import abc

import qrtutils
from database.const import CONFIG_PATH, MODULE_NAME, MODULE_PATH
from qrtconst import REL_PATH_HEADER

def submodule_initialization(db_name, path, submod_depth=1):
    '''
    对子模块根据配置进行初始化设置

    Parameter
    ---------
    db_name: string
        数据库子模块名称，必须与配置文件对应
    path: string
        调用模块所在文件夹的路径，通常为dirname(__file__)，用于将相对路径转化为绝对路径，
        此处假设子模块文件夹的母文件夹为主模块文件夹
    submod_depth: int, default 1
        子模块在模块文件夹的深度(在获取主模块文件夹路径时使用)
        例如: 主模块文件夹为c:/main_module，子模块文件夹为c:/main_module/sub_module
        则此时submod_depth=1，以此类推

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
            module_path = sep.join(path.split(sep)[:-submod_depth])
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
        logger的名称，若没有启用日志，则返回None
    '''
#     if not log_config['enable_log']:
#         return None
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

def set_db_logger():
    '''
    设置主数据库的日志

    Return
    ------
    logger_name: string
        logger的名称，若没有启用日志，则返回None
    '''
    logger_config = qrtutils.parse_config(CONFIG_PATH)['log']
    logger_config['name'] = MODULE_NAME
    # 相对路径转绝对路径
    path = logger_config['log_path']
    if path.startswith(REL_PATH_HEADER):
        logger_config['log_path'] = qrtutils.relpath2abs(MODULE_PATH, path)
    return set_logger(logger_config)

class DBEngine(object, metaclass=abc.ABCMeta):
    '''
    数据引擎虚基类
    定义以下接口:
    query: 类方法，依据给定的参数，从数据文件中查询相应的数据
    insert: 类方法，依据给定的参数，向数据文件中插入数据
    remove_data: 类方法，将数据库中给定的数据删除
    move_to: 类方法，将给定的数据移动到其他给定的位置
    '''
    @classmethod
    @abc.abstractmethod
    def query(cls, *args, **kwargs):
        '''
        使用数据引擎从数据文件或者数据库中获取数据
        '''
        pass

    @classmethod
    @abc.abstractmethod
    def insert(cls, *args, **kwargs):
        '''
        通过数据引擎将数据插入到数据文件或者数据库中，返回布尔值显示是否插入成功
        '''
        pass

    @classmethod
    @abc.abstractmethod
    def remove_data(cls, params):
        '''
        将给定的数据删除
        '''
        pass

    @classmethod
    @abc.abstractmethod
    def move_to(cls, src_params, dest_params):
        '''
        将给定的数据移动到另一给定的位置
        '''
        pass

    @abc.abstractmethod
    def _parse_path(self):
        '''
        解析数据的绝对路径
        '''
        pass

