# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:53:33 2018

@author: Hao Li

数据库核心实现

"""
import abc

import pandas as pd
import h5py
import numpy as np

from database.hdf5db.const import PANEL, TIME_SERIES


class DataIndex(object, metaclass=abc.ABCMeta):
    '''
    抽象基类，用于定义轴对象的接口
    原始构造函数用于对常量和变量进行初始化，该类提供两个额外的构造函数
    其中:
        init_from_dataset(h5py.Dataset: date_dset)将使用h5py的Dataset对象作为参数传入
        init_from_index(pandas.Index: pd_index)将使用pandas.Index对象作为参数传入
        to_bytes()可以将pandas.Index(datetime)转换为二进制字符串数组(numpy.array)
    '''
    @abc.abstractclassmethod
    def init_from_dataset(cls, dset):
        '''
        使用数据集对象对数据进行初始化

        Parameter
        ---------
        dset: h5py.Dataset
            数据集对象
        '''
        pass

    @abc.abstractclassmethod
    def init_from_index(cls, pd_index):
        '''
        使用索引对象对TimeIndex进行初始化

        Parameter
        ---------
        pd_index: pandas.Index
            索引对象
        '''
        pass

    @abc.abstractmethod
    def to_types(self, dtype, *args, **kwargs):
        '''
        将索引数据对象转化为二进制字符串序列

        Parameter
        ---------
        dtype: string
            numpy可接受的字符串byte类型
        args: iterable
            其他顺序参数
        kwargs: dict like
            其他关键字参数

        Return
        ------
        datas: np.array
        '''
        pass


class TimeIndex(DataIndex):
    '''
    时间轴，内部通过pandas.Index对象来对时间数据进行维护

    原始构造函数用于对常量和变量进行初始化，该类提供两个额外的构造函数
    其中:
        init_from_dataset(h5py.Dataset: date_dset)将使用h5py的Dataset对象作为参数传入
        init_from_index(pandas.Index: pd_index)将使用pandas.Index对象作为参数传入
        to_bytes()可以将pandas.Index(datetime)转换为二进制字符串数组(numpy.array)
    该类还提供三个只读属性获取相关的元数据，包括:
        length: int，数据库中时间数据的长度
        start_time: datetime like，数据的开始时间
        end_time: datetime like，数据的最新时间
    '''

    def __init__(self):
        self._length = None
        self._end_time = None
        self._start_time = None
        self._data = None

    @classmethod
    def init_from_dataset(cls, date_dset):
        '''
        使用数据集对象对TimeIndex进行初始化

        Parameter
        ---------
        date_dset: h5py.Dataset
            数据集对象，具有length(int)、latest_data_time(string)、start_time(string)三个属性

        Return
        ------
        obj: TimeIndex
            经过数据集数据初始化后的TimeIndex对象
        '''
        obj = cls()
        dates = date_dset[...]
        obj._data = pd.Index(pd.to_datetime([s.decode('utf-8') for s in dates]))
        obj._length = date_dset.attrs['length']
        obj._end_time = pd.to_datetime(date_dset.attrs['latest_data_time'])
        obj._start_time = pd.to_datetime(date_dset.attrs['start_time'])
        return obj

    @classmethod
    def init_from_index(cls, pd_index):
        '''
        使用索引对象对TimeIndex进行初始化

        Parameter
        ---------
        pd_index: pandas.Index
            索引对象，元素为datetime以及其子类的实例

        Return
        ------
        obj: TimeIndex
            经过pd.Index对象初始化后的TimeIndex对象
        '''
        obj = cls()
        obj._data = pd_index.sort_values()
        obj._length = len(pd_index)
        obj._start_time = pd_index[0]
        obj._end_time = pd_index[-1]

    def to_bytes(self, dtype, date_fmt):
        '''
        将索引数据对象转化为二进制字符串序列

        Parameter
        ---------
        dtype: string
            numpy可接受的字符串byte类型
        date_fmt: string
            将时间转化为字符串的格式(供strftime调用)

        Return
        ------
        dates: np.array
        '''
        if not dtype.lower().startswith('s'):
            raise ValueError("Wrong dtype for date string, given {}".format(dtype))
        out = self._data.strftime(date_fmt).astype(dtype)
        return out

    @property
    def length(self):
        return self._length

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def data(self):
        return self._data


class SymbolIndex(DataIndex):
    '''
    代码轴，内部通过pd.Index对象来对代码数据进行维护

    除了基本接口外，还提供一个额外属性:
        length: int, 代码数据的长度
    '''

    def __init__(self):
        self._data = None
        self._length = None

    @classmethod
    def init_from_dataset(cls, symbol_dset):
        '''
        使用数据集对象对SymbolIndex进行初始化

        Parameter
        ---------
        symbol_dset: h5py.Dataset
            数据集对象，包含length(int)属性

        Return
        ------
        obj: SymbolIndex
        '''
        obj = cls()
        data = symbol_dset[...]
        obj._data = pd.Index([s.decode('utf-8') for s in data])
        obj._length = symbol_dset.attrs['length']
        return obj

    @classmethod
    def init_from_index(cls, pd_index):
        '''
        使用索引对象对SymbolIndex进行初始化

        Parameter
        ---------
        pd_index: pandas.Index
            索引对象

        Return
        ------
        obj: SymbolIndex
        '''
        obj = cls()
        obj._data = pd_index
        obj._length = len(pd_index)
        return obj

    def to_bytes(self, dtype):
        '''
        将索引数据对象转化为二进制字符串序列

        Parameter
        ---------
        dtype: string
            numpy可接受的字符串byte类型

        Return
        ------
        dates: np.array
        '''
        if not dtype.lower().startswith('s'):
            raise ValueError("Wrong dtype for date string, given {}".format(dtype))
        out = np.array(self._data, dtype=dtype)
        return out

    @property
    def length(self):
        return self._length

    @property
    def data(self):
        return self._data


class Data(object):
    '''
    数据类，内部通过pd.DataFrame(面板数据)或者pd.Series(时间序列)维护数据

    提供两个初始化的接口:
        init_from_pd: 从pd.DataFrame(或者pd.Series)对数据对象进行初始化
        init_from_datasets: 从数据、时间轴数据和代码轴数据(如果是面板数据)
    另外提供一些其他方法:
        decompose2datasets: 将内部数据分解为数据、时间轴数据和代码轴数据(如果是面板数据)
        rearrange_symbol: 将代码数据按照给定顺序进行排列
        drop_before_date: 将给定时间之前的数据删去
        merge: 将给定的数据与当前数据在时间轴上进行融合
    '''

    def __init__(self):
        self._data_category = None  # 用于记录当前数据是TS还是CS
        self._data = None

    def init_from_pd(self, pd_data):
        '''
        使用pd.DataFrame(或者pd.Series)对数据进行初始化

        Parameter
        ---------
        pd_data: pd.DataFrame or pd.Series
            若为pd.DataFrame，则数据的格式为，index为时间轴，columns为代码轴
            若为pd.Series，则数据的格式为index为时间轴
        '''
        if isinstance(pd_data, pd.DataFrame):
            self._data_category = PANEL
        elif isinstance(pd_data, pd.Series):
            self._data_category = TIME_SERIES
        else:
            raise ValueError("Only pd.DataFrame or pd.Series is allowed, you provide {}".
                             format(type(pd_data)))
        self._data = pd_data

    def init_from_datasets(self, data_dset, date_dset, symbol_dset=None):
        '''
        使用数据集数据对对象进行初始化

        Parameter
        ---------
        data_dset: h5py.Dataset
            存储核心数据的数据集
        date_dset: h5py.Dataset
            存储时间数据的数据集
        symbol_dset: h5py.Dataset, default None
            存储代码数据的数据集，如果该参数为None，表示当前数据分类为时间序列
        '''
        date_index = TimeIndex.init_from_dataset(date_dset)
        if symbol_dset is not None:
            self._data_category = PANEL
            symbol_index = SymbolIndex.init_from_dataset(symbol_dset)
            value = data_dset[:, :symbol_index.length]
            data = pd.DataFrame(value, index=date_index.data, columns=symbol_index.data)
        else:
            self._data_category = TIME_SERIES
            value = data_dset[:]
            data = pd.Series(value, index=date_index)
