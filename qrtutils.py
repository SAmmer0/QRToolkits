# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 15:45:24 2018

@author: Hao Li

辅助工具模块
"""
import json
from os.path import join, expanduser
from os import sep
from copy import deepcopy
import re
import pdb
import logging
from sys import stdout


from qrtconst import REL_PATH_HEADER_LEN, REL_PATH_HEADER


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
    if path.startswith('~'):
        path = expanduser(path)
    return path


def trans_config_pathes(config):
    '''
    递归将所有配置的路径(配置选项中包含path关键字)转化为系统分隔符模式

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


def set_logger(log_config, module_path, logger_name):
    '''
    通用Logger设置

    Parameter
    ---------
    log_config: dict
        日志设置配置，需要包含以下选项
        "log_to_file": boolean, 是否把日志写入文件
        "log_path": string, 如果log_to_file为True，则需要提供日志的写入路径，可以为相对路径
        "log_level": string, logging模块可接受的日志记录等级
        "format": string, 日志记录的格式
        "date_format": string, 日志中时间的记录格式
    module_path: string
        模块所在的文件夹的路径
    logger_name: string
        Logger的名称

    Return
    ------
    logger_name: string
    '''
    log_config = trans_config_pathes(log_config)
    logger = logging.getLogger(logger_name)
    if logger.handlers:     # 当前仅支持每个Logger一个处理函数
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


if __name__ == '__main__':
    config = parse_config(r'E:\QRToolkits\database\config.json')
