# -*- encoding: utf-8
'''
JSON存储引擎核心实现部分
'''
import logging
import json
from copy import deepcopy

import pandas as pd

from database.utils import DBEngine
from database.jsonEngine.const import LOGGER_NAME, DB_CONFIG, DataFormatCategory, FilledStatus, NaS, SUFFIX

# 获取当前日志处理的句柄
logger = logging.getLogger(LOGGER_NAME)

# 函数
def trans_metadata(json_metadata):
    '''
    将从JSON文件中读取出来的元数据进行转换
    
    Parameter
    ---------
    json_metadata: dict
    
    Return
    ------
    meta_data: dict
    '''
    meta_data = deepcopy(json_metadata)
    meta_data['start time'] = pd.to_datetime(meta_data['start time'])
    meta_data['end time'] = pd.to_datetime(meta_data['end time'])
    meta_data['data category'] = DataFormatCategory[meta_data['data category']]
    meta_data['filled status'] = FilledStatus[meta_data['filled status']]

# 类

class DataWrapper(object):
    '''
    数据包装类，用于处理数据文件与pandas对象之间的转换
    该类提供以下功能:
    init_from_pd: 从pandas对象对实例进行初始化
    init_from_files: 从文件对象对实例进行初始化
    rearrange_symbol: 将本实例中的数据的代码顺序按照给定的顺序排列(仅支持面板数据)
    drop_before_date: 将给定日期(包含该日期)的数据剔除
    update: 利用另一实例对该实例的数据进行更新
    split_data: 将数据分解为更小的键值对应的实例，键为文件名，值为包含对应数据的实例
    to_jsonformat: 将内部数据转换为可以存储到json文件的形式
    '''
    def __init__(self):
        '''
        数据初始化
        '''
        self._data = None
        self._data_category = None
    
    @classmethod
    def init_from_pd(cls, pd_data):
        '''
        使用pandas对象对实例进行初始化
        
        Parameter
        ---------
        pd_data: pandas.Series or pandas.DataFrame
        
        Return
        ------
        obj: DataWrapper
        '''
        obj = cls()
        if isinstance(pd_data, pd.DataFrame):
            obj._data_category = DataFormatCategory.PANEL
        elif isinstance(pd_data, pd.Series):
            obj._data_category = DataFormatCategory.TIME_SERIES
        else:
            raise TypeError("Only pandas.DataFrame or pandas.Series is supported!")
        obj._data = pd_data        
        return obj
    
    @classmethod
    def init_from_files(cls, data_file_objs, meta_file_obj):
        '''
        使用文件对象对实例进行初始化
        
        Parameter
        ---------
        data_file_objs: iterable
            数据文件对象容器，元素为file object
        meta_file_obj: file object
            元数据文件对象
        
        Return
        ------
        obj: DataWrapper
        '''
        obj = cls()
        meta_data = trans_metadata(json.load(meta_file_obj))
        obj._data_category = meta_data['data category']

        def load_data(file_obj, symbol=None):
            # 从数据文件中加载数据
        
