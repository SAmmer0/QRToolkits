#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/18
"""
import importlib
from os import sep, listdir
from os.path import isfile as op_isfile
from os.path import join as op_join
from collections import deque

from pitdata.const import CONFIG
from database.const import REL_PATH_SEP

# --------------------------------------------------------------------------------------------------
# 缓存
data_dictionary_cache = None

# --------------------------------------------------------------------------------------------------
# 功能函数


def file2object(file_path):
    '''
    读取计算文件，将其转化为数据描述类的对象

    Parameter
    ---------
    file_path: string
        计算文件所在的路径

    Return
    ------
    obj: pitdata.utils.DataDescription

    Notes
    -----
    计算文件必须要包含一个名为dd的DataDescription对象
    '''
    module_name = file_path.split(sep)[-1].split('.')[0]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    obj = getattr(module, 'dd')
    return obj


def load_all():
    '''
    加载所有计算文件，返回计算文件字典，并检查是否有重复名称的文件，如有则报错

    Return
    ------
    out: dict
        数据字典，格式为{name: {'data_description': dd, 'rel_path': rel_path}}
    '''
    nonlocal data_dictionary_cache
    if data_dictionary_cache is not None:
        return data_dictionary_cache
    root_path = CONFIG['data_description_file_path']
    queue = deque()
    queue.append((root_path, ''))    # (abs_path, rel_path)
    out = {}
    while len(queue) > 0:
        abs_path, rel_path = queue.pop()
        if op_isfile(abs_path):
            obj = file2object(abs_path)
            if obj.name in out:
                raise IndexError('Duplicate data name!(duplication={})'.format(obj.name))
            out[obj.name] = {'data_description': obj,
                             'rel_path': REL_PATH_SEP.join([rel_path, obj.name])}
        else:
            for f in listdir(abs_path):
                queue.append((op_join(abs_path, f), REL_PATH_SEP.join([rel_path, f])))
    data_dictionary_cache = out
    return out


def find_data_description(name):
    '''
    查找给定名称的数据对应的数据描述对象及相对路径

    Parameter
    ---------
    name: string
        数据名称

    Return
    ------
    dd: pitdata.utils.DataDescription
    rel_path: string
    '''
    nonlocal data_dictionary_cache
    if data_dictionary_cache is None:
        data_dictionary_cache = load_all()
    res = data_dictionary_cache[name]
    return res['data_description'], res['rel_path']
