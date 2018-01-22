# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:53:33 2018

@author: Hao Li

数据库核心实现

"""
import abc
import logging
from os import remove
import pdb

import pandas as pd
import h5py
import numpy as np

from database.hdf5db.const import LOGGER_NAME, DB_CONFIG, DataCate, FilledStatus, NaS
from database.hdf5db.exceptions import InvalidInputTypeError, UnsupportDataTypeError

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
        logger.debug("TimeIndex.init_from_dataset")
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
        logger.debug("TimeIndex.init_from_index")
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
        logger.debug("TimeIndex.to_bytes")
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
        logger.debug("SymbolIndex.init_from_dataset")
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
        logger.debug("SymbolIndex.init_from_index")
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
        logger.debug("SymbolIndex.to_bytes")
        if not dtype.lower().startswith('s'):
            raise InvalidInputTypeError("Wrong dtype for date string, given {}".format(dtype))
        out = np.array(self._data, dtype=dtype)
        return out

    @property
    def length(self):
        return self._length

    @property
    def data(self):
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
        使用(排序后的)pd.DataFrame(或者pd.Series)对数据进行初始化

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
        logger.debug("Data.init_from_pd")
        obj = cls()
        if isinstance(pd_data, pd.DataFrame):
            obj._data_category = DataCate.PANEL
        elif isinstance(pd_data, pd.Series):
            obj._data_category = DataCate.TIME_SERIES
        else:
            raise UnsupportDataTypeError("Only pd.DataFrame or pd.Series is allowed, you provide {}".
                                         format(type(pd_data)))
        obj._data = pd_data.sort_index()
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
        logger.debug("Data.init_from_datasets")
        obj = cls()
        date_index = TimeIndex.init_from_dataset(date_dset)
        if symbol_dset is not None:
            obj._data_category = DataCate.PANEL
            symbol_index = SymbolIndex.init_from_dataset(symbol_dset)
            value = data_dset[:, :symbol_index.length]
            data = pd.DataFrame(value, index=date_index.data, columns=symbol_index.data)
        else:
            obj._data_category = DataCate.TIME_SERIES
            value = data_dset[:]
            data = pd.Series(value, index=date_index.data)
        obj._data = data.sort_index()
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
        logger.debug("Data.decompose2dataset")
        data = self._data.values
        date_index = TimeIndex.init_from_index(self._data.index)
        if self._data_category == DataCate.PANEL:
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
        logger.debug("Data.rearrange_symbol")
        if self._data_category == DataCate.TIME_SERIES:
            raise NotImplementedError

        diff = sorted(self._data.columns.difference(symbol_order))
        new_order = list(symbol_order) + diff
        self._data = self._data.reindex(columns=new_order)

    def drop_before_date(self, date):
        '''
        将数据中在给定日期前(包括该日期)的数据都剔除

        date: datetime like
            日期剔除的阈值
        '''
        logger.debug("Data.drop_before_date")
        date = pd.to_datetime(date)
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
        logger.debug("Data.update")
        if self.data_category != other.data_category:
            raise ValueError('Incompatible data category, {dc} required, but {pdc} is provided'.
                             format(dc=self.data_category, pdc=other.data_category))
        if not other.start_time <= self.start_time <= other.end_time <= self.end_time:
            import warnings
            warnings.warn('Improper time order', RuntimeWarning)
            return
        self.drop_before_date(other.end_time)
        if self.data_category == DataCate.PANEL:
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
        logger.debug("Data.sort_index")
        self._data = self._data.sort_index(ascending=ascending)

    def as_type(self, dtype):
        '''
        将数据类型转换为给定的类型

        Parameter
        ---------
        dtype: np.dtype like
            数据类型标识
        '''
        logger.debug("Data.as_type")
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
        if self.data_category == DataCate.PANEL:
            return self._data.columns
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


class DBConnector(object):
    '''
    数据库文件管理类

    Parameter
    ---------
    path: string
        数据文件的存储路径

    Notes
    -----
    模块说明：
    主要提供数据文件初始化功能、文件自动扩容功能、查询和插入数据功能，具体功能如下：
        create_datafile: 类方法，如果数据文件不存在，则根据提供的参数和配置，创建一个新文件
        init_from_file: 类方法，使用文件对数据库对象进行初始化
        query_all: 返回数据库中的所有数据
        query: 返回特定时间的数据
        insert: 将给定的数据插入到数据库中
        reshape_colsize: 重置数据库文件中列的大小
    使用方法：
        第一次初始化可以使用create_datafile方法，该函数将创建一个数据文件，并进行相关初始化属性的设置，
        以后可以直接通过init_from_file方法对数据库进行初始化，如果没有找到对应的文件，将引发FileNotFoundError，
        通常可以先尝试从文件中初始化，然后捕捉对应的异常，在异常中创建文件
        如果需要插入数据直接调用insert即可，若需要查询数据，使用query即可
    注意:
        正常的数据插入将会对应更新对象的相应属性，但是如果出现了reshape_colsize的情况，将会创建新的
        数据文件，通过新的对象来对数据文件进行维护，因此当前的对象已经不再适用
    '''

    def __init__(self, path):
        self._path = path
        self._data_category = None
        self._dtype = None
        self.property = None   # 字典，记录数据库文件的所有属性
        self.symbols = None     # 用于记录当前数据的股票代码，TIME_SERIES的该属性为None

    def init_from_object(self, obj):
        '''
        从其他数据库对象进行初始化，主要用于解决重新创建文件后无法保持对文件的跟踪的问题

        Parameter
        ---------
        obj: DBConnector
        '''
        logger.debug("DBConnector.init_from_object")
        from copy import deepcopy
        self._path = obj._path
        self._data_category = obj._data_category
        self._dtype = obj._dtype
        self.property = deepcopy(obj.property)
        if obj.symbols is not None:
            self.symbols = obj.symbols.copy()

    @classmethod
    def create_datafile(cls, path, data_category, dtype=DB_CONFIG['default_data_type'],
                        init_col_size=DB_CONFIG['initial_col_size']):
        '''
        创建数据文件，并对数据文件进行初始化，返回对应的数据文件对象

        Parameter
        ---------
        path: string
            文件的路径
        data_category: DataCate
            数据类别，目前支持[PANEL, TIME_SERIES]
        dtype: np.dtype
            数据类型
        init_col_size: int or float
            初始化时列的长度

        Return
        ------
        obj: DBConnector
            包含初始信息的数据库对象
        '''
        logger.debug("DBConnector.create_datafile")
        obj = cls(path)
        valid_category = [DataCate.PANEL, DataCate.TIME_SERIES]
        if data_category not in valid_category:
            raise InvalidInputTypeError('Unsupport data category, expect {exp}, you provide {yp}'.
                                        format(exp=valid_category, yp=data_category))
        obj._data_category = data_category
        dtype = np.dtype(dtype)
        if str(dtype)[0].lower() not in DB_CONFIG['valid_type_header']:
            raise InvalidInputTypeError('Unsupported data type')
        obj._dtype = dtype

        # 文件初始化
        with h5py.File(obj._path, 'w-') as store:
            # 时间数据初始化
            store.create_dataset('time', shape=(1, ), maxshape=(None, ),
                                 dtype=DB_CONFIG['date_dtype'])
            store['time'].attrs['length'] = 0
            store['time'].attrs['latest_data_time'] = NaS
            store['time'].attrs['start_time'] = NaS
            # 属性初始化
            store.attrs['filled status'] = FilledStatus.EMPTY.name
            store.attrs['data category'] = obj._data_category.name
            obj.property = {'time': {'length': 0, 'latest_data_time': None, 'start_time': None},
                            'filled status': FilledStatus.EMPTY,
                            'data category': obj._data_category}
            if obj._data_category == DataCate.PANEL:   # 面板数据初始化
                store.create_dataset('symbol', shape=(1, ), maxshape=(None, ),
                                     dtype=DB_CONFIG['symbol_dtype'])
                store.create_dataset('data', shape=(1, init_col_size),
                                     maxshape=(None, init_col_size), chunks=(1, init_col_size),
                                     dtype=obj._dtype)
                store.attrs['column size'] = init_col_size
                store['symbol'].attrs['length'] = 0
                store['data'].attrs['dtype'] = str(obj._dtype)
                store['data'][...] = np.nan
                obj.property.update({'column size': init_col_size,
                                     'symbol': {'length': 0},
                                     'data': {'dtype': obj._dtype}})
            else:
                store.create_dataset('data', shape=(1, ), maxshape=(None, ),
                                     dtype=obj._dtype)
                store.attrs['column size'] = 1
                store['data'].attrs['dtype'] = str(obj._dtype)
                store['data'][...] = np.nan
                obj.property.update({'column size': 1, 'data': {'dtype': obj._dtype}})
        return obj

    @classmethod
    def init_from_file(cls, path):
        '''
        从数据文件对数据库对象进行初始化，如果无法找到文件则会触发FileNotFoundError

        Parameter
        ---------
        path: string
            文件所在的路径

        Return
        ------
        obj: DBConnector
            经过数据文件初始化后的数据库对象
        '''
        logger.debug("DBConnector.init_from_file")
        obj = cls(path)
        try:
            store = h5py.File(path, 'r')
            obj._data_category = DataCate[store.attrs['data category']]
            obj._dtype = store['data'].attrs['dtype']
            time_dset = store['time']
            obj.property = {'time': {'length': store['time'].attrs['length'],
                                     'latest_data_time': pd.to_datetime(time_dset.attrs['latest_data_time']),
                                     'start_time': pd.to_datetime(time_dset.attrs['start_time'])},
                            'filled status': FilledStatus[store.attrs['filled status']],
                            'data category': DataCate[store.attrs['data category']],
                            'column size': store.attrs['column size'],
                            'data': {'dtype': np.dtype(obj._dtype)}}
            if obj._data_category == DataCate.PANEL:
                obj.property.update({'symbol': {'length': store['symbol'].attrs['length']}})
                obj.symbols = SymbolIndex.init_from_dataset(store['symbol'])
        except OSError:
            raise FileNotFoundError
        return obj

    def query_all(self):
        '''
        返回当前数据集中的所有数据，并根据数据的分类返回恰当的格式，PANEL->pd.DataFrame，
        TS->pd.Series

        Return
        ------
        out: pd.DataFrame or pd.Series
            数据文件中存储的所有数据，若没有填充任何数据，则返回None
        '''
        logger.debug("DBConnector.query_all")
        try:
            if self.property['filled status'] == FilledStatus.EMPTY:
                return None
            store = h5py.File(self._path, 'r')
            # 加载日期数据
            date_dset = store['time']
            if self._data_category == DataCate.PANEL:
                symbol_dset = store['symbol']
                data_dset = store['data']
                data = Data.init_from_datasets(data_dset, date_dset, symbol_dset)
                out = data.data
            elif self._data_category == DataCate.TIME_SERIES:
                data_dset = store['data']
                data = Data.init_from_datasets(data_dset, date_dset)
                out = data.data
            else:
                raise NotImplementedError
        finally:
            store.close()
        return out

    def _query_panel(self, start_time, end_time):
        '''
        请求时间序列的数据(包含首尾)

        Parameter
        ---------
        start_time: datetime like
            开始时间
        end_time: datetime like
            结束时间

        Return
        ------
        out: pd.DataFrame or pd.Series
            若数据分类为PANEL，则返回pd.DataFrame，反之如果为TIME_SERIES则返回pd.Series，若没有符合
            要求的数据，返回None
        '''
        logger.debug("DBConnector._query_panel")
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)
        all_data = self.query_all()
        if all_data is not None:
            mask = (all_data.index <= end_time) & (all_data.index >= start_time)
            out = all_data.loc[mask]
            if len(out) <= 0:
                return None
        else:
            out = None
        return out

    def _query_cs(self, ptime):
        '''
        请求横截面数据，仅支持PANEL类型的数据

        ptime: datetime like
            横截面数据时间点

        Return
        ------
        out: pd.Series
            横截面数据，若没有则返回None
        '''
        logger.debug("DBConnector._query_cs")
        if self._data_category == DataCate.TIME_SERIES:
            raise NotImplementedError
        all_data = self.query_all()
        if all_data is None:
            return None
        try:
            out = all_data.loc[ptime]
        except KeyError:
            out = None
        return out

    def query(self, start_time, end_time=None):
        '''
        统一数据查询接口

        Parameter
        ---------
        start_time: datetime like
            开始时间
        end_time: datetime like, default None
            结束时间，若该参数为None，表示要获取横截面数据

        Return
        ------
        out: pd.Series or pd.DataFrame
            返回数据的类型根据查询结果而定
        '''
        logger.debug("DBConnector.query")
        if end_time is None:
            out = self._query_cs(start_time)
        else:
            out = self._query_panel(start_time, end_time)
        return out

    def insert(self, data):
        '''
        将数据插入到数据库文件中

        Parameter
        ---------
        data: pd.DataFrame or pd.Series
            数据要求为数值格式(dtype以f或者i开头)，其中index为时间，要求为datetime类型，columns为
            代码列表(如果数据为pd.DataFrame)
        '''
        logger.debug("DBConnector.insert")
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
        data = data.astype(self._dtype)
        if isinstance(data, pd.DataFrame):
            self._insert_df(data)
        elif isinstance(data, pd.Series):
            self._insert_series(data)
        else:
            raise NotImplementedError

    def _insert_df(self, data):
        '''
        将pd.DataFrame格式的数据插入到数据库文件中，仅支持PANEL类型

        Parameter
        ---------
        data: pd.DataFrame
        '''
        logger.debug("DBConnector._insert_df")
        if self._data_category != DataCate.PANEL:
            raise UnsupportDataTypeError('{dt} is not supported by {dc}'.
                                         format(dt=type(data), dc=DataCate.PANEL.name))
        data = Data.init_from_pd(data)  # 获取数据
        obj_property = self.property
        if len(data.symbol_index) > obj_property['column size']:    # 数据列超过文件列容量
            import warnings
            warnings.warn('Data column needs to be resized!', RuntimeWarning)
            self.reshape_colsize(data)
            return
        if obj_property['filled status'] == FilledStatus.FILLED:   # 非第一次更新
            data.rearrange_symbol(self.symbols.data.tolist())   # 对数据进行重新排列
            # 对数据进行切割
            data.drop_before_date(obj_property['time']['latest_data_time'])
            if len(data) == 0:  # 没有数据需要插入
                return
        data_arr, data_index, data_symbol = data.decompose2dataset()

        start_index = obj_property['time']['length']
        end_index = start_index + len(data)
        with h5py.File(self._path, 'r+') as store:
            data_dset = store['data']
            time_dset = store['time']
            symbol_dset = store['symbol']
            # 重新分配容量，填入数据
            data_dset.resize((end_index, obj_property['column size']))
            data_dset[start_index: end_index, :] = np.nan
            data_dset[start_index: end_index, :len(data.symbol_index)] = data_arr

            time_dset.resize((end_index, ))
            time_dset[start_index: end_index] = data_index.to_bytes(DB_CONFIG['date_dtype'],
                                                                    DB_CONFIG['db_time_format'])
            symbol_dset.resize((len(data.symbol_index), ))
            symbol_dset[...] = data_symbol.to_bytes(DB_CONFIG['symbol_dtype'])
            # 更新相应的属性数据
            if obj_property['filled status'] == FilledStatus.EMPTY:
                store.attrs['filled status'] = FilledStatus.FILLED.name
                time_dset.attrs['start_time'] = data_index.start_time\
                    .strftime(DB_CONFIG['db_time_format'])
                # 更新property
                obj_property['filled status'] = FilledStatus.FILLED
                obj_property['time']['start_time'] = data_index.start_time

            time_dset.attrs['latest_data_time'] = data_index.end_time\
                .strftime(DB_CONFIG['db_time_format'])
            time_dset.attrs['length'] = end_index
            symbol_dset.attrs['length'] = len(data.symbol_index)

            obj_property['time']['latest_data_time'] = data_index.end_time
            obj_property['time']['length'] = end_index
            obj_property['symbol']['length'] = len(data.symbol_index)
        self.symbols = SymbolIndex.init_from_index(data.symbol_index)

    def _insert_series(self, data):
        '''
        将pd.Series格式的数据插入到数据库文件中，仅支持TIME_SERIES类型

        Parameter
        ---------
        data: pd.Series
        '''
        logger.debug("DBConnector._insert_series")
        if self._data_category != DataCate.TIME_SERIES:
            raise InvalidInputTypeError('pd.Series data is expected, you provide {}'.
                                        format(type(data)))
        obj_property = self.property
        data = Data.init_from_pd(data)
        if obj_property['filled status'] == FilledStatus.FILLED:    # 非第一次插入数据
            data.drop_before_date(obj_property['time']['latest_data_time'])
            if len(data) == 0:
                return
        data_arr, data_index, _ = data.decompose2dataset()
        start_index = obj_property['time']['length']
        end_index = start_index + len(data)
        with h5py.File(self._path, 'r+') as store:
            data_dset = store['data']
            time_dset = store['time']
            data_dset.resize((end_index, ))
            data_dset[start_index: end_index] = data_arr

            time_dset.resize((end_index, ))
            time_dset[start_index: end_index] = data_index.to_bytes(DB_CONFIG['date_dtype'],
                                                                    DB_CONFIG['db_time_format'])
            # 更新属性数据
            if obj_property['filled status'] == FilledStatus.EMPTY:
                store.attrs['filled status'] = FilledStatus.FILLED.name
                time_dset.attrs['start_time'] = data_index.start_time\
                    .strftime(DB_CONFIG['db_time_format'])
                # 更新property
                obj_property['filled status'] = FilledStatus.FILLED
                obj_property['time']['start_time'] = data_index.start_time

            time_dset.attrs['latest_data_time'] = data_index.end_time\
                .strftime(DB_CONFIG['db_time_format'])
            time_dset.attrs['length'] = end_index

            obj_property['time']['latest_data_time'] = data_index.end_time
            obj_property['time']['length'] = end_index

    def reshape_colsize(self, data):
        '''
        对原数据文件进行列扩容操作，仅支持PANEL，扩容程度根据配置文件进行调整

        Parameter
        ---------
        data: Data
            需要添加的数据
        '''
        logger.debug("DBConnector.reshape_colsize")
        if self._data_category == DataCate.TIME_SERIES:
            raise NotImplementedError
        if self.property['filled status'] == FilledStatus.FILLED:    # 非第一次插入数据是出现容量不足
            db_data = self.query_all()
            if db_data is not None:
                db_data = Data.init_from_pd(db_data)
                data.update(db_data)
        remove(self._path)
        new_size = self.property['column size'] + DB_CONFIG['col_size_increase_step']
        obj = DBConnector.create_datafile(self._path, self._data_category, self._dtype, new_size)
        self.init_from_object(obj)
        self.insert(data.data)
