#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/17

高级数据管理工具，依赖于模块中的基础功能
"""
from os.path import join, exists, dirname, isdir
from os import sep, makedirs, listdir, remove
import logging
from shutil import move, rmtree

from datautils import DataView
from tdtools import get_calendar
from database.const import REL_PATH_SEP
from pitdata.query import query
from pitdata.io import list_all_data, move_data, delete_data
from pitdata.const import CONFIG, LOGGER_NAME, METADATA_FILENAME
from pitdata.updater.operator import dump_metadata, load_metadata
from pitdata.updater.loader import load_all
from pitdata.updater.order import DependencyTree

# --------------------------------------------------------------------------------------------------
# 预处理
# 设置日志
logger = logging.getLogger(LOGGER_NAME)

# --------------------------------------------------------------------------------------------------
# 数据缓存获取类
class PITDataCache(object):
    '''
    工厂类，用于生成和获取数据缓存
    数据缓存的唯一标志是：数据名称+数据的预加载数量

    Example
    -------
    >>> from pitdata.tools import pitcache_getter
    >>> beta_cache = pitcache_getter('BETA', 100)    # pitcache_getter(data_name, preload_num)
    >>> data = beta_cache.get_csdata('2018-04-19')
    '''
    __cache = {}
    def __call__(self, name, preload_num):
        '''
        加载给定名称和预加载数量的数据

        Parameter
        ---------
        name: string
            数据名称
        preload_num: int
            预加载数据的数量，必须为正整数

        Return
        ------
        cache: datautils.DataView
        '''
        if preload_num < 0:
            raise ValueError('Parameter \"preload_num\" cannot be negetative!')
        if not isinstance(preload_num, int):
            raise TypeError('The type of parameter \"preload_num\" must be int!')
        cache_name = name + '_{:03d}'.format(preload_num)    # 缓存的唯一标志为name+preload_num，例如"BETA_100"
        if cache_name in self.__cache:
            return self.__cache[cache_name]
        else:
            func = lambda st, et: query(name, st, et)
            cache = DataView(func, get_calendar('stock.sse'), preload_num=preload_num)
            self.__cache[cache_name] = cache
            return cache

pitcache_getter = PITDataCache()    # 此处并未将类设计成单例模式，考虑到可以由很多独立的缓存，但提供一个常用的缓存

# --------------------------------------------------------------------------------------------------
# 数据移动和删除功能
def cf_relpath2abspath(rel_path):
    '''
    将计算文件的相对路径转换为绝对路径

    Parameter
    ---------
    rel_path: string
        计算文件相对路径

    Return
    ------
    abs_path: string
    '''
    computing_fp = CONFIG['data_description_file_path']
    abs_path = rel_path.replace(REL_PATH_SEP, sep)
    return join(computing_fp, abs_path) + '.py'

def delete_empty_folder(path, file_filter=('__pycache__', )):
    '''
    检测当前文件夹是否为空，如果为空则删除

    Parameter
    ---------
    path: string
        文件夹绝对路径
    file_filter: tuple, default ('__pycache__',)
        文件过滤，被包含的文件(或者文件夹)均会被忽视

    Return
    ------
    result: boolean
        若删除，则返回True，未删除返回False
    '''
    if not isdir(path):
        raise ValueError('Input path is not a folder(path={})!'.format(path))
    resid_files = listdir(path)
    resid_files = set(resid_files).difference(file_filter)
    if resid_files:    # 文件夹中还有文件
        logger.info('[Operation=delete_empty_folder, Info=\"Try to delete a non-empty folder(path={}).\"]'.format(path))
        return False
    try:
        rmtree(path)
    except Exception as e:
        logger.exception(e)
        return False
    logger.info('[Operation=delete_empty_folder, Info=\"An empty folder is deleted(path={}).\"]'.format(path))
    delete_empty_folder(dirname(path), file_filter=file_filter)    # 递归删除上层空文件夹
    return True

def move_computing_file(name, dest):
    '''
    移动计算文件到新的(相对)位置

    Parameter
    ---------
    name: string
        数据名称
    dest: string
        新位置的相对路径，新路径的数据名称要与之前的数据相同

    Return
    ------
    result: boolean
        移动成功返回True
    '''
    all_data = list_all_data()
    if name not in all_data:
        logger.warning('[Operation=move_computing_file, Info=\"Try to move a data({}) that does not exist!\"]'.format(name))
        return False
    source_relpath = all_data[name]['rel_path']
    if dest == source_relpath:
        logger.warning('[Operation=move_computing_file, Info=\"Source path and destination path are the same(data={})!\"'.format(name))
        return False
    if dest.split(REL_PATH_SEP)[-1] != name:
        logger.warning('[Operation=move_computing_file, Info=\"Destination data name(path={d}) is not consistent with origin(path={s}).\"]'.
                       format(d=dest, s=source_relpath))
        return False
    source_abspath = cf_relpath2abspath(source_relpath)
    dest_abspath = cf_relpath2abspath(dest)
    dest_name = dest.split(REL_PATH_SEP)[-1]
    if not exists(dirname(dest_abspath)):
        makedirs(dirname(dest_abspath))
    try:
        move(source_abspath, dest_abspath)
        move_data(source_relpath, dest, all_data[name]['datatype'])
        logger.info('[Operation=move_computing_file, Info=\"Moving data from {s} to {d}.\"]'.format(s=source_relpath, d=dest))
        delete_empty_folder(dirname(source_abspath))
        if dest_name != name:    # 新数据的名称与原数据不同，需要修改数据库中元数据文件
            metadata = load_metadata(METADATA_FILENAME)
            mdata = metadata[name]
            del metadata[name]
            metadata[dest_name] = mdata
            dump_metadata(metadata, METADATA_FILENAME)
    except Exception as e:
        logger.exception(e)
        return False
    return True

def delete_computing_file(name, delete_branch=True):
    '''
    删除给定数据的计算文件

    Parameter
    ---------
    name: string
        数据名称
    delete_branch: boolean, default True
        删除所有对该数据具有依赖的数据

    Return
    ------
    result: boolean
        删除成功返回True
    '''
    all_data = list_all_data()
    if name not in all_data:
        logger.warning('[Operation=delete_computing_file, Info=\"Try to delete a data({}) that does not exist!\"]'.format(name))
        return False
    data_relpath = all_data[name]['rel_path']
    data_abspath = cf_relpath2abspath(data_relpath)

    if delete_branch:    # 删除所有对当前节点具有依赖的节点
        dd = load_all()
        dd_tree = DependencyTree(dd)
        for child in dd_tree.get_branch(name):
            if child != name:
                delete_computing_file(child, False)
    try:
        remove(data_abspath)
        delete_data(data_relpath, all_data[name]['datatype'])
        logger.info('[Operation=delete_computing_file, Info=\"Delete data(path={}) successfully.\"]'.format(data_relpath))
        delete_empty_folder(dirname(data_abspath))
        metadata = load_metadata(METADATA_FILENAME)
        del metadata[name]
        dump_metadata(metadata, METADATA_FILENAME)
    except Exception as e:
        logger.exception(e)
        return False
    return True
