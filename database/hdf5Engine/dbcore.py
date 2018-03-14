# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:53:33 2018

@author: Hao Li

数据库核心实现

"""
import abc
import logging
from os import remove, makedirs, sep
from os.path import exists, dirname, join
import pdb

import pandas as pd
import h5py
import numpy as np

from database.hdf5Engine.const import (LOGGER_NAME, DB_CONFIG, DataFormatCategory,
                                       FilledStatus, NaS, SUFFIX, REL_PATH_SEP)
from database.hdf5Engine.exceptions import InvalidInputTypeError, UnsupportDataTypeError
from database.utils import DBEngine

# 获取当前日志句柄
logger = logging.getLogger(LOGGER_NAME)


class DataIndex(object, metaclass=abc.ABCMeta):
    '''
    抽象基类，用于定义轴对象的接口
    原始构造函数用于对常量和变量进行初始化，该类提供两个额外的构造函数
    其中:
        init_from_dataset(h5py.Dataset: date_dset)将使用h5py的Dataset对象作为参数传入
        init_from_index(pandas.Index: pd_index)将使用pandas.Index对象作为参数传入
        to_bytes()可以将pandas.Index(datetime)转换为二进制字符串数组(numpy.array)
    '''
    @classmethod
    @abc.abstractmethod
    def init_from_dataset(cls, dset):
        '''
        使用数据集对象对数据进行初始化

        Parameter
        ---------
        dset: h5py.Dataset
            数据集对象
        '''
        pass

    @classmethod
    @abc.abstractmethod
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
    def to_bytes(self, dtype, *args, **kwargs):
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
        obj._data = pd_index
        obj._length = len(pd_index)
        obj._start_time = pd_index[0]
        obj._end_time = pd_index[-1]
        return obj

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
            raise InvalidInputTypeError("Wrong dtype for date string, given {}".format(dtype))
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
        # 外部还是可以通过一定的方法改变
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
            raise InvalidInputTypeError("Wrong dtype for date string, given {}".format(dtype))
        out = np.array(self._data, dtype=dtype)
        return out

    @property
    def length(self):
        return self._length

    @property
    def data(self):
        # 外部还是可以通过一定的方法改变
        return self._data

    def copy(self):
        obj = SymbolIndex()
        obj._data = self._data.copy()
        obj._length = self._length
        return obj


class Data(object):
    '''
    数据类，内部通过pd.DataFrame(面板数据)或者pd.Series(时间序列)维护数据

    提供两个初始化的接口:
        init_from_pd: 从pd.DataFrame(或者pd.Series)对数据对象进行初始化
        init_from_datasets: 从数据、时间轴数据和代码轴数据(如果是面板数据)
    另外提供一些其他方法:
        decompose2dataset: 将内部数据分解为数据、时间轴数据和代码轴数据(如果是面板数据)
        rearrange_symbol: 将代码数据按照给定顺序进行排列
        drop_before_date: 将给定时间之前的数据删去
        update: 将给定的数据与当前数据在时间轴上进行融合更新
    '''

    def __init__(self):
        self._data_category = None  # 用于记录当前数据是TS还是CS
        self._data = None

    @classmethod
    def init_from_pd(cls, pd_data):
        '''
        使用pd.DataFrame(或者pd.Series)对数据进行初始化

        Parameter
        ---------
        pd_data: pd.DataFrame or pd.Series
            若为pd.DataFrame，则数据的格式为，index为时间轴，columns为代码轴
            若为pd.Series，则数据的格式为index为时间轴

        Return
        ------
        obj: Data
            通过pd.DataFrame初始化后的对象
        '''
        obj = cls()
        if isinstance(pd_data, pd.DataFrame):
            obj._data_category = DataFormatCategory.PANEL
        elif isinstance(pd_data, pd.Series):
            obj._data_category = DataFormatCategory.TIME_SERIES
        else:
            raise UnsupportDataTypeError("Only pd.DataFrame or pd.Series is allowed, you provide {}".
                                         format(type(pd_data)))
        # 还是需要把时间排序加上去，因为时间序列类数据中时间性质很重要，而且已经很明确了该数据库
        # 存储的是带有时间标签的数据，因此存储前需要保证时间顺序的正确性，读取则不需要，因为读取时的
        # 顺序由写入的顺序决定
        obj._data = pd_data.sort_index(ascending=True)
        return obj

    @classmethod
    def init_from_datasets(cls, data_dset, date_dset, symbol_dset=None):
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

        Return
        ------
        obj: Data
            通过数据集数据初始化后的对象
        '''
        obj = cls()
        date_index = TimeIndex.init_from_dataset(date_dset)
        if symbol_dset is not None:
            obj._data_category = DataFormatCategory.PANEL
            symbol_index = SymbolIndex.init_from_dataset(symbol_dset)
            value = data_dset[:, :symbol_index.length]
            data = pd.DataFrame(value, index=date_index.data, columns=symbol_index.data)
        else:
            obj._data_category = DataFormatCategory.TIME_SERIES
            value = data_dset[:]
            data = pd.Series(value, index=date_index.data)
        obj._data = data
        return obj

    def decompose2dataset(self):
        '''
        将内部pd.DataFrame(或者pd.Series)格式的数据分解为数据、时间轴数据和代码轴数据(如果是面板数据)

        Return
        ------
        data: np.ndarray or np.array
            核心数据，0轴(纵向)表示时间轴，1轴(横向)表示代码轴(如果是面板数据)
        date_index: TimeIndex
            时间数据
        symbol_index: SymbolIndex
            代码数据，如果原始数据为时间序列数据则该返回值为None
        '''
        data = self._data.values
        date_index = TimeIndex.init_from_index(self._data.index)
        if self._data_category == DataFormatCategory.PANEL:
            symbol_index = SymbolIndex.init_from_index(self._data.columns)
        else:
            symbol_index = None
        return data, date_index, symbol_index

    def rearrange_symbol(self, symbol_order):
        '''
        将数据按照给定的代码顺序对数据列重新排列(仅面板数据有该方法)，排列机制如下：
        1. 优先按照参数提供的顺序排列，如果参数提供的代码多于当前数据的代码，则对数据按照参数提供的代码
        进行填充
        2. 如果数据的代码多于参数的代码，对于多出的代码进行排序(升序)，然后添加在参数代码顺序后面
        3. 如果参数代码和数据的代码互相的差运算都不是空集，则先按照1进行填充，然后按照2进行补充

        Parameter
        ---------
        symbol_order: iterable
            给定的代码排列顺序
        '''
        if self._data_category == DataFormatCategory.TIME_SERIES:
            raise NotImplementedError
        if self._data.columns.tolist() == list(symbol_order):
            return
        diff = sorted(self._data.columns.difference(symbol_order), reverse=False)
        new_order = list(symbol_order) + diff
        self._data = self._data.reindex(columns=new_order)

    def drop_before_date(self, date):
        '''
        将数据中在给定日期前(包括该日期)的数据都剔除

        date: datetime like
            日期剔除的阈值
        '''
        date = pd.to_datetime(date)
        if date < self.start_time:  # 无重叠时间
            return
        data = self._data
        self._data = data.loc[data.index > date]

    def update(self, other):
        '''
        将数据与其他数据在时间轴上融合，重叠的数据均以参数为准(时间轴和代码轴)，具体融合的规则如下:
        1. 如果两个数据时间上没有交集，则直接在时间轴上连接
        2. 如果两个数据时间有交集，且交集的部分为self.start_time到other.end_time这一段，则重复部分以
            提供的数据为准
            (此时时间对应关系为other.start_time<=self.start_time<=other.end_time<=self.end_time)
        3. 其他情形，均不做处理，并发出相关警告

        Parameter
        ---------
        other: Data
            更新数据的来源，要求提供的参数与当前对象的数据分类相同
        '''
        if self.data_category != other.data_category:
            raise ValueError('Incompatible data category, {dc} required, but {pdc} is provided'.
                             format(dc=self.data_category, pdc=other.data_category))
        if not other.start_time <= self.start_time <= other.end_time <= self.end_time:
            import warnings
            warnings.warn('Improper time order', RuntimeWarning)
            return
        self.drop_before_date(other.end_time)
        if self.data_category == DataFormatCategory.PANEL:
            self.rearrange_symbol(other.symbol_index)
            other.rearrange_symbol(self.symbol_index)   # 保证合并后的顺序一致
        data = pd.concat([other.data, self._data], axis=0)
        if data.index.has_duplicates:
            raise ValueError('Duplicated time index')
        self._data = data

    def sort_index(self, ascending=True):
        '''
        对时间轴进行排序

        Parameter
        ---------
        ascending: boolean, default True
            是否升序排列，默认为True
        '''
        self._data = self._data.sort_index(ascending=ascending)

    def trans2type(self, dtype):
        '''
        将数据类型转换为给定的类型

        Parameter
        ---------
        dtype: np.dtype like
            数据类型标识
        '''
        self._data = self._data.astype(dtype)

    @property
    def data_category(self):
        '''
        数据分类
        '''
        return self._data_category

    @property
    def start_time(self):
        '''
        数据开始时间
        '''
        return self._data.index[0]

    @property
    def end_time(self):
        '''
        数据结束时间
        '''
        return self._data.index[-1]

    @property
    def symbol_index(self):
        if self.data_category == DataFormatCategory.PANEL:
            return self._data.columns.tolist()
        else:
            return None

    @property
    def data(self):
        '''
        返回内部数据的副本
        '''
        return self._data.copy()

    def __len__(self):
        return len(self._data)


class Reader(object):
    '''
    读取器类，负责从数据文件中读取数据
    该类提供几个对外的接口:
    query: 根据给定的参数，从数据文件中请求给定的数据
    query_all: 根据给定的参数，从数据文件中请求文件中存储的所有数据
    '''
    def __init__(self, params):
        self.properties = None
        self._params = params
        self.symbols = None
        self._load_property()

    def _load_property(self):
        '''
        从数据文件中加载元数据
        '''
        def load_time(t):
            '''
            处理加载初始化数据文件时间参数的问题
            '''
            return t if t == NaS else pd.to_datetime(t)
        try:
            store = h5py.File(self._params.absolute_path, 'r')
            time_dset = store['time']
            self.properties = {'time': {'length': store['time'].attrs['length'],
                                     'latest_data_time': load_time(time_dset.attrs['latest_data_time']),
                                     'start_time': load_time(time_dset.attrs['start_time'])},
                            'filled status': FilledStatus[store.attrs['filled status']],
                            'data category': DataFormatCategory[store.attrs['data category']],
                            'column size': store.attrs['column size'],
                            'data': {'dtype': np.dtype(store['data'].attrs['dtype'])}}
            if self._params.store_fmt[2] == DataFormatCategory.PANEL: # 这里假设第三级存储格式为DataFromatCategory类型
                self.properties.update({'symbol': {'length': store['symbol'].attrs['length']}})
                self.symbols = SymbolIndex.init_from_dataset(store['symbol'])
        except OSError:
            raise FileNotFoundError
        except Exception as e:
            store.close()
            raise e

    def query_all(self):
        '''
        根据传入给本对象的相关参数，从数据文件中查询所有数据，并根据数据的分类返回恰当的格式，PANEL->pd.DataFrame，
        TS->pd.Series

        Return
        ------
        out: pd.DataFrame or pd.Series
            数据文件中存储的所有数据，若没有填充任何数据，则返回None
        '''
        try:
            tmp_properties = self.properties
            if tmp_properties['filled status'] == FilledStatus.EMPTY:
                logger.warn("[Operation=Reader.query_all, Info=\"Query an empty data file(file_path = {}).\"]".
                            format(self._params.absolute_path))
                return None
            store = h5py.File(self._params.absolute_path, 'r')
            # 加载日期数据
            date_dset = store['time']
            if tmp_properties['data category'] == DataFormatCategory.PANEL:
                symbol_dset = store['symbol']
                data_dset = store['data']
                data = Data.init_from_datasets(data_dset, date_dset, symbol_dset)
                out = data.data
            elif tmp_properties['data category'] == DataFormatCategory.TIME_SERIES:
                data_dset = store['data']
                data = Data.init_from_datasets(data_dset, date_dset)
                out = data.data
            else:
                raise NotImplementedError
        finally:
            store.close()
        return out

    def query(self):
        '''
        根据传入给本对象的相关参数，从数据文件中查询请求的数据，并根据数据的分类返回恰当的格式，PANEL->pd.DataFrame，
        TS->pd.Series

        Return
        ------
        out: pd.DataFrame or pd.Series
            数据文件中存储的所有数据，若没有填充任何数据，则返回None
        '''
        params = self._params
        if (self.properties['data category'] == DataFormatCategory.PANEL and
            params.start_time is not None and params.end_time is None):  # 请求横截面数据
            all_data = self.query_all()
            try:
                out = all_data.loc[params.start_time]
            except KeyError:
                return None
        elif params.start_time is not None and params.end_time is not None:
            all_data = self.query_all()
            if all_data is not None:
                mask = (all_data.index >= params.start_time) & (all_data.index <= params.end_time)
                out = all_data.loc[mask]
                if len(out) == 0:
                    return None
            else:
                return None
        else:
            raise NotImplementedError
        return out


class Writer(object):
    '''
    写入类，负责向文件中写入数据
    该类提供几个对外接口:
    insert: 将给定的数据插入到数据文件中
    '''
    def __init__(self, params):
        self._params = params
        try:
            self._reader = Reader(params)
        except FileNotFoundError:
            self._create_datafile()
            self._reader = Reader(params)

    def _mk_dirs(self):
        '''
        调用系统函数创建文件夹
        '''
        directory_path = dirname(self._params.absolute_path)
        if not exists(directory_path):
            makedirs(directory_path)

    def _create_datafile(self, col_size=DB_CONFIG['initial_col_size']):
        '''
        当数据文件不存在时，调用创建并初始化文件

        Parameter
        ---------
        col_size: int
            初始的列数，仅对面板数据(DataFormatCategory.PANEL)有效，默认值为配置文件的initial_col_size确定
        '''
        params = self._params
        if str(params.dtype)[0].lower() not in DB_CONFIG['valid_type_header']:
            raise InvalidInputTypeError('Unsupported data type')

        # 文件初始化
        self._mk_dirs()
        with h5py.File(params.absolute_path, 'w-') as store:
            # 时间数据初始化
            store.create_dataset('time', shape=(1, ), maxshape=(None, ),
                                 dtype=DB_CONFIG['date_dtype'])
            store['time'].attrs['length'] = 0
            store['time'].attrs['latest_data_time'] = NaS
            store['time'].attrs['start_time'] = NaS
            # 属性初始化
            store.attrs['filled status'] = FilledStatus.EMPTY.name
            store.attrs['data category'] = params.store_fmt[2].name
            if params.store_fmt[2] == DataFormatCategory.PANEL:   # 面板数据初始化
                store.create_dataset('symbol', shape=(1, ), maxshape=(None, ),
                                     dtype=DB_CONFIG['symbol_dtype'])
                store.create_dataset('data', shape=(1, col_size),
                                     maxshape=(None, col_size), chunks=(1, col_size),
                                     dtype=params.dtype)
                store.attrs['column size'] = col_size
                store['symbol'].attrs['length'] = 0
                store['data'].attrs['dtype'] = str(params.dtype)
                store['data'][...] = np.nan
            else:
                store.create_dataset('data', shape=(1, ), maxshape=(None, ),
                                     dtype=params.dtype)
                store.attrs['column size'] = 1
                store['data'].attrs['dtype'] = str(params.dtype)
                store['data'][...] = np.nan

    def insert(self, data):
        '''
        将数据插入到数据文件中

        Parameter
        ---------
        data: pandas.DataFrame or pandas.Series
            需要插入的数据，index为时间轴，columns(对于pandas.DataFrame)为股票代码轴

        Return
        ------
        result: boolean
        '''
        if not isinstance(data, (pd.DataFrame, pd.Series)):
            raise NotImplementedError
        dtypes = data.dtypes
        if hasattr(dtypes, '__iter__'):  # pd.DataFrame
            dtypes = dtypes.astype(str)
            is_valid_type = any([np.all(dtypes.str.startswith(s))
                                 for s in DB_CONFIG['valid_type_header']])
        else:   # pd.Series
            is_valid_type = any([str(dtypes).startswith(s)
                                 for s in DB_CONFIG['valid_type_header']])
        if not is_valid_type:
            raise UnsupportDataTypeError('Data type is not supported!')
        # data = data.astype(self._dtype)
        if isinstance(data, pd.DataFrame):
            return self._insert_df(data)
        else:
            return self._insert_series(data)


    def _insert_series(self, data):
        '''
        向数据文件中插入pandas.Series数据

        Parameter
        ---------
        data: pandas.Series
            Index为时间

        Return
        ------
        result: boolean
            是否成功插入数据
        '''
        if not isinstance(data, pd.Series):
            raise TypeError("pandas.Series expected, while {} is provided!".format(type(data)))
        params = self._params
        reader = self._reader
        data = Data.init_from_pd(data)
        data.trans2type(params.dtype)
        if reader.properties['filled status'] == FilledStatus.FILLED:
            data.drop_before_date(reader.properties['time']['latest_data_time'])
            if len(data) == 0:
                return False
        result = False
        data_arr, data_index, _ = data.decompose2dataset()
        start_index = reader.properties['time']['length']
        end_index = start_index + len(data)
        with h5py.File(params.absolute_path, 'r+') as store:
            data_dset = store['data']
            time_dset = store['time']
            data_dset.resize((end_index,))
            data_dset[start_index: end_index] = data_arr

            time_dset.resize((end_index, ))
            time_dset[start_index: end_index] = data_index.to_bytes(DB_CONFIG['date_dtype'],
                                                                    DB_CONFIG['db_time_format'])
            # 更新元数据
            new_properties = {'time': {'length': end_index,
                                       'latest_data_time': data.end_time,
                                       'start_time': data.start_time if reader.properties['time']['start_time'] == NaS else reader.properties['time']['start_time']},
                              'filled status': FilledStatus.FILLED}
            self._update_file_metadata(store, new_properties)
            result = True
        self._update_reader()
        return result


    def _insert_df(self, data):
        '''
        向数据文件中插入pandas.DataFrame文件

        Parameter
        ---------
        data: pandas.DataFrame
            index为时间轴，columns为股票代码轴

        Return
        ------
        result: boolean
            是否成功插入数据
        '''
        if not isinstance(data, pd.DataFrame):
            raise TypeError("pandas.DataFrame expected, while {} is provided!".format(type(data)))
        params = self._params
        data = Data.init_from_pd(data)
        data.trans2type(params.dtype)
        reader = self._reader
        reader_properties = reader.properties
        if len(data.symbol_index) > reader_properties['column size']:
            import warnings
            warnings.warn('Data column needs to be resized!', RuntimeWarning)
            target_colsize = self._get_target_size(len(data.symbol_index))
            return self._reshape_insertdf(target_colsize, data)
        if reader_properties['filled status'] == FilledStatus.FILLED: # 非第一次更新
            data.rearrange_symbol(reader.symbols.data.tolist()) # 对代码轴重新排列
            # 对数据进行切割
            data.drop_before_date(reader_properties['time']['latest_data_time'])
            if len(data) == 0:  # 没有数据需要插入，返回插入数据失败
                return False
        result = False
        data_arr, data_index, data_symbol = data.decompose2dataset()
        start_index = reader_properties['time']['length']
        end_index = start_index + len(data)
        with h5py.File(params.absolute_path, 'r+') as store:
            data_dset = store['data']
            time_dset = store['time']
            symbol_dset = store['symbol']
            # 重新分配容量
            data_dset.resize((end_index, reader_properties['column size']))
            time_dset.resize((end_index, ))
            symbol_dset.resize((len(data.symbol_index), ))
            # 插入数据
            # 主数据
            data_dset[start_index: end_index, :] = np.nan
            data_dset[start_index: end_index, :len(data.symbol_index)] = data_arr
            # 时间数据
            time_dset[start_index: end_index] = data_index.to_bytes(DB_CONFIG['date_dtype'],
                                                                    DB_CONFIG['db_time_format'])
            # 股票代码数据
            symbol_dset[...] = data_symbol.to_bytes(DB_CONFIG['symbol_dtype'])
            # 更新元数据
            new_properties = {'time': {'length': end_index,
                                       'latest_data_time': data.end_time,
                                       'start_time': data.start_time if reader_properties['time']['start_time'] == NaS else reader_properties['time']['start_time']},
                              'filled status': FilledStatus.FILLED,
                              'symbol': {'length': len(data.symbol_index)}}
            self._update_file_metadata(store, new_properties)
            result = True
        self._update_reader()
        return result

    def _get_target_size(self, data_colsize):
        '''
        计算新的数据文件需要扩展的列的数量

        Parameter
        ---------
        data_colsize: int
            新的数据列的长度

        Return
        ------
        target_size: int
            按照配置文件递增得到的新的列长度
        '''
        target_size = self._reader.properties['column size']
        iter_num = 0    # 为了避免配置文件错误导致死循环的问题
        while target_size <= data_colsize:
            iter_num += 1
            if iter_num > 100:
                raise ValueError('Infinite loop while calculating target column size, please check the config file!')
            target_size += DB_CONFIG['col_size_increase_step']
        return target_size

    def _reshape_insertdf(self, target_colsize, data):
        '''
        插入的数据列数超过了文件可容纳的列容量，需要对数据文件重新设置，然后将数据插入到文件中，
        保持数据顺序等属性与原数据库中的数据一致

        Parameter
        ---------
        target_colsize: int
            目标列数
        data: Data
            需要插入的数据

        Return
        ------
        result: boolean
            是否成功插入数据
        '''
        if self._reader.properties['filled status'] == FilledStatus.FILLED:
            db_data = self._reader.query_all()
            if db_data is not None:
                db_data = Data.init_from_pd(db_data)
                data.update(db_data)
        remove(self._params.absolute_path)
        self._create_datafile(target_colsize)
        self._update_reader()
        return self._insert_df(data.data)

    def _update_reader(self):
        '''
        对内部Reader对象的状态进行更新
        '''
        self._reader = Reader(self._params)

    def _update_file_metadata(self, store, new_properties):
        '''
        将元数据更新到文件中

        Parameter
        ---------
        store: h5py.File
            需要被写入元数据的文件
        new_properties: dict
            需要被更新的属性，基本结构如下
            {'time': {'length': time lenght,
                    'latest_data_time': data end time,
                    'start_time': data start time},
           'filled status': new filled status,
           'symbol': {'length': symbol length}(optional)}
        '''
        store['time'].attrs['length'] = new_properties['time']['length']
        store['time'].attrs['latest_data_time'] = new_properties['time']['latest_data_time'].strftime(DB_CONFIG['db_time_format'])
        store['time'].attrs['start_time'] = new_properties['time']['start_time'].strftime(DB_CONFIG['db_time_format'])
        store.attrs['filled status'] = new_properties['filled status'].name
        symbol_property = new_properties.get('symbol', None)
        if symbol_property is not None:
            store['symbol'].attrs['length'] = symbol_property['length']



class HDF5Engine(DBEngine):
    '''
    主引擎类
    提供以下接口:
    query: 类方法，依据给定的参数，从数据文件中查询相应的数据
    insert: 类方法，依据给定的参数，向数据文件中插入数据
    remove_data: 类方法，将数据库中给定的数据删除
    move_to: 类方法，将给定的数据移动到其他给定的位置
    '''
    def __init__(self, params):
        self._params = params
        self._parse_path()
        self._reader = None
        self._writer = None

    @classmethod
    def query(cls, params):
        '''
        从数据库文件中请求给定的数据

        Parameter
        ---------
        params: database.db.ParamsParser
            其中，若end_time属性为None时，表示请求横截面数据，仅对面板类型数据有效

        Return
        ------
        out: pandas.DataFrame or pandas.Series
        '''
        obj = cls(params)
        obj._load_reader()
        return obj._reader.query()

    @classmethod
    def insert(cls, data, params):
        '''
        将给定的数据插入到数据文件中

        Parameter
        ---------
        data: pandas.DataFrame or pandas.Series
        params: database.db.ParamsParser
            相关参数设置

        Return
        ------
        result: boolean
        '''
        obj = cls(params)
        obj._load_writer()
        return obj._writer.insert(data)

    @classmethod
    def remove_data(cls, params):
        '''
        将给定的数据删除

        Parameter
        ---------
        params: database.db.ParamsParser

        Return
        ------
        result: boolean
        '''
        try:
            obj = cls(params)
            remove(obj._params.absolute_path)
        except Exception as e:
            logger.exception(e)
            return False
        return True


    @classmethod
    def move_to(cls, src_params, dest_params):
        '''
        将给定的数据移动到其他给定的位置

        Parameter
        ---------
        src_params: database.db.ParamsParser
            原数据文件相关设定参数
        dest_params: database.db.ParamsParser
            目标数据文件相关设定参数
        '''
        from shutil import move
        dest_obj = cls(dest_params)
        if exists(dest_obj._params.absolute_path):  # 目标数据已经存在
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


    def _load_reader(self):
        '''
        加载Reader对象
        '''
        self._reader = Reader(self._params)

    def _load_writer(self):
        '''
        加载Writer对象
        '''
        self._writer = Writer(self._params)

    def _parse_path(self):
        '''
        解析数据绝对路径
        '''
        params = self._params
        rel_path = params.rel_path.replace(REL_PATH_SEP, sep) + SUFFIX
        params.set_absolute_path(join(params.main_path, rel_path))
