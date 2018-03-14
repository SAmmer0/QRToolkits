#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/13
"""
from os import sep, makedirs, remove
from os.path import join, exists, dirname
import pickle
import logging
from shutil import move

from database.utils import DBEngine
from database.pickleEngine.const import SUFFIX, REL_PATH_SEP, ENCODING, PICKLE_PROTOCOL, LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

class PickleEngine(DBEngine):
    '''
    Pickle存储引擎，主要用于存储对象类的数据
    提供以下接口：
    insert: 将给定数据存入到文件中
    query: 从给定的数据文件中查询所有数据
    move_to: 将给定数据移动到其他位置
    remove_data: 将给定数据删除

    Parameter
    ---------
    params: database.db.ParamsParser
    '''
    def __init__(self, params):
        self._params = params
        self._parse_path()

    def _parse_path(self):
        '''
        将相对路径解析为绝对路径
        '''
        params = self._params
        rel_path = params.rel_path.replace(REL_PATH_SEP, sep) + SUFFIX
        params.set_absolute_path(join(params.main_path, rel_path))

    @classmethod
    def insert(cls, data, params):
        '''
        向数据库中插入给定的数据

        Parameter
        ---------
        data: object
        params: database.db.ParamsParser

        Return
        ------
        result: boolean
        '''
        db_obj = cls(params)
        try:
            path = db_obj._params.absolute_path
            if not exists(dirname(path)):
                makedirs(dirname(path))
            with open(path, 'wb') as f:
                pickle.dump(data, f, protocol=PICKLE_PROTOCOL)
        except Exception as e:
            logger.exception(e)
            return False
        return True

    @classmethod
    def query(cls, params):
        '''
        从数据库中查询给定的数据

        Parameter
        ---------
        params: database.db.ParamsParser

        Return
        ------
        out: object
        '''
        db_obj = cls(params)
        with open(db_obj._params.absolute_path, 'rb') as f:
            data = pickle.load(f)
        return data

    @classmethod
    def remove_data(cls, params):
        '''
        从数据库中删除给定的数据

        Parameter
        ---------
        params: database.db.ParamsParser

        Return
        ------
        result: boolean
        '''
        db_obj = cls(params)
        try:
            remove(db_obj._params.absolute_path)
        except Exception as e:
            logger.exception(e)
            return False
        return True

    @classmethod
    def move_to(cls, src_params, dest_params):
        '''
        将给定的数据一个位置移动到其他位置

        Parameter
        ---------
        src_params: database.db.ParamsParser
        dest_params: database.db.ParamsParser

        Return
        ------
        result: boolean
        '''
        dest_obj = cls(dest_params)
        if exists(dest_obj._params.absolute_path):
            raise ValueError('Cannot move to an existing position!')
        src_obj = cls(src_params)
        try:
            if not exists(dirname(dest_obj._params.absolute_path)):
                makedirs(dirname(dest_obj._params.absolute_path))
            move(src_obj._params.absolute_path, dest_obj._params.absolute_path)
        except Exception as e:
            logger.exception(e)
            return False
        return True
