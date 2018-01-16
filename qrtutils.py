# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 15:45:24 2018

@author: Hao Li

辅助工具模块
"""
import json
from os.path import join
from os import sep
from copy import deepcopy
import re
import pdb
from functools import wraps

from qrtconst import REL_PATH_HEADER_LEN


def parse_config(config_path):
    '''
    对配置文件进行解析

    Parameter
    ---------
    config_path: string
        配置文件所在的路径

    Return
    ------
    out: dict
        配置数据字典
    '''
    with open(config_path, 'r') as f:
        config = re.sub('\n\s*//.*', '', f.read())
        config = json.loads(config)
        config = trans_config_pathes(config)
        return config


def trans_config_sep(path):
    '''
    检测系统路径分隔符是否为'/'，如果不是则将自定义的路径分隔符转换为系统分隔符

    Parameter
    ---------
    path: string
        需要被转换的路径，可以是绝对路径，也可以是相对路径

    Return
    ------
    out_path: string
        转换后的路径
    '''
    if sep != '/':
        path = path.replace('/', sep)
    return path


def trans_config_pathes(config):
    '''
    递归将所有配置的路径转化为系统分隔符模式

    Parameter
    ---------
    config: dict
        配置文件

    Return
    ------
    out_config: dict
        路径格式转换后的配置文件
    '''
    out_config = deepcopy(config)
    for k, v in out_config.items():
        if isinstance(v, dict):
            out_config[k] = trans_config_pathes(v)
        else:
            if 'path' in k:
                out_config[k] = trans_config_sep(v)
    return out_config


def relpath2abs(parent_folder, rel_path):
    '''
    将相对路径转换为绝对路径

    Parameter
    ---------
    parent_folder: string
        母文件夹的绝对路径
    rel_path: string
        相对路径，以'./'开头的被视为相对路径

    Return
    ------
    path: string
        绝对路径
    '''
    rel_path = trans_config_sep(rel_path)
    return join(parent_folder, rel_path[REL_PATH_HEADER_LEN:])


def debug_wrapper(logger):
    '''
    装饰器，用于向给定日志中打印相关调用和参数信息

    Parameter
    ---------
    logger: logging.Logger
        日志句柄

    Return
    ------
    wrapper: function
        装饰后的函数
    '''
    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            logger.debug('call ' + func.__name__ + ' with args = ' +
                         repr(args) + ', kwargs = ' + repr(kwargs))
            return func(*args, **kwargs)
        return inner
    return wrapper


if __name__ == '__main__':
    config = parse_config(r'E:\QRToolkits\database\config.json')
