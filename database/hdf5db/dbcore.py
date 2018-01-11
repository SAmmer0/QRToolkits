# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:53:33 2018

@author: Hao Li

数据库核心实现

"""
import pandas as pd
import h5py

class TimeIndex(object):
    '''
    时间轴，内部通过pandas.Index对象来对时间数据进行维护
    
    原始构造函数用于对常量和变量进行初始化，该类提供两个额外的构造函数
    其中:
        init_from_dataset(h5py.Dataset: date_dset)将使用h5py的Dataset对象作为参数传入
        init_from_index(pandas.Index: pd_index)将使用pandas.Index对象作为参数传入
        to_byte()可以将pandas.Index(datetime)转换为二进制字符串数组(numpy.array)
    该类还提供三个只读属性获取相关的元数据，仅从Dataset中读取的数据才有，由Index构造的数据都为None
    包括:
        length: int，数据库中时间数据的长度
        latest_data_time: datetime like，数据的最新时间
        start_time: datetime like，数据的开始时间
    '''
    def __init__(self):
        self._length = None
        self._latest_data_time = None
        self._start_time = None
        self._data = None
    
    def init_from_dataset(self, date_dset):
        '''
        使用数据集对象对TimeIndex进行初始化
        
        Parameter
        ---------
        date_dset: h5py.Dataset
            数据集对象，具有length(int)、latest_data_time(string)、start_time(string)三个属性
        '''
        dates = date_dset[...]
        self._data = pd.Index(pd.to_datetime([s.decode('utf-8') for s in dates]))
        self._length = date_dset.attrs['length']
        self._latest_data_time = date_dset.attrs['latest_data_time']
        self._start_time = date_dset.attrs['start_time']
    
    def init_from_index(self, pd_index):
        '''
        使用索引对象对TimeIndex进行初始化
        
        Parameter
        ---------
        pd_index: pandas.Index
            索引对象，元素为datetime以及其子类的实例
        '''
        self._data = pd_index
    
    def to_byte(self, fmt):
        '''
        将索引数据对象转化为二进制字符串序列
        
        Parameter
        ---------
        fmt: string
            numpy可接受的字符串类型，由配置文件确定
        
        Return
        ------
        dates: np.array
        '''