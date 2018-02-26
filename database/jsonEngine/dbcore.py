# -*- encoding: utf-8
'''
JSON存储引擎核心实现部分
'''
import logging
import json
from copy import deepcopy
from functools import reduce
from os import sep, makedirs
from os.path import join, exists
from shutil import move, rmtree

import pandas as pd

from database.utils import DBEngine
from database.jsonEngine.const import (LOGGER_NAME,
                                       DB_CONFIG,
                                       DataFormatCategory,
                                       FilledStatus,
                                       NaS,
                                       SUFFIX,
                                       METADATA_FILENAME)

# 获取当前日志处理的句柄
logger = logging.getLogger(LOGGER_NAME)

# -------------------------------------------------------------------------------------------------------------
# 函数


def date2filename(date):
    '''
    将给定的日期转化为对应的文件名，转化的方式依赖于配置文件
    若频率为YEAR，则文件名类似为2018
    若频率为MONTH，则文件名类似为201802
    若频率为QUARTER，则文件名类似为2018Q1

    Parameter
    ---------
    date: datetime like

    Return
    ------
    out: string

    Example
    -------
    假设频率为月份
    >>> date2filename('2010-01-03')
    '201001'
    >>> date2filename('2018-04-05')
    '201804'
    '''
    date = pd.to_datetime(date)
    if DB_CONFIG['data_spilt_frequency'] == 'YEAR':
        return date.strftime('%Y')
    elif DB_CONFIG['data_spilt_frequency'] == 'MONTH':
        return date.strftime('%Y%m')
    elif DB_CONFIG['data_spilt_frequency'] == 'QUARTER':
        year = date.year
        quarter = date.quarter
        return str(year) + 'Q' + str(quarter)
    else:
        raise NotImplementedError

def date2filenamelist(start_time, end_time):
    '''
    将起始日期转化为一系列数据文件名
    实现过程中假设只有两级变化，类似于时钟只有时针和分针
    实现过程中假设基本的文件格式为'(year number)(identifier(optional))(subperiod number(optional))'

    Parameter
    ---------
    start_time: datetime like
    end_time: datetime like

    Return
    ------
    out: list
        元素为文件名

    Example
    -------
    假设频率为季度，即QUARTER
    >>> date2filenamelist('2018-01-01', '2018-04-01')
    ['2018Q1', '2018Q2']
    >>> date2filenamelist('2017-01-01', '2018-01-01')
    ['2017Q1', '2017Q2', '2017Q3', '2017Q4', '2018Q1']
    '''
    cycle_len_map = {'MONTH': 12, 'QUARTER': 4}
    start_file = date2filename(start_time)
    end_file = date2filename(end_time)
    start_year = int(start_file[:4])
    end_year = int(end_file[:4])
    if DB_CONFIG['data_spilt_frequency'] == 'YEAR':
        return [str(i) for i in range(start_year, end_year+1)]
    else:
        identifier = ''.join(s for s in start_file[4:] if not s.isdigit())
        num_len = len(start_file) - 4 - len(identifier)
        minor_format = '%0{}d'.format(num_len)
        minor_start = int(start_file[4+len(identifier):])
        minor_end = int(end_file[4+len(identifier):])
        cycle_len = cycle_len_map[DB_CONFIG['data_spilt_frequency']]
        year = start_year
        minor = minor_start
        out = []
        while year < end_year or (year == end_year and minor <= minor_end):
            tmp = str(year) + identifier + minor_format % minor
            out.append(tmp)
            minor += 1
            if minor > cycle_len:
                year += 1
                minor = 1
        return out



# -------------------------------------------------------------------------------------------------------------
# 类

class DataWrapper(object):
    '''
    数据包装类，用于处理数据文件与pandas对象之间的转换
    该类提供以下功能:
    init_from_pd: 从pandas对象对实例进行初始化
    init_from_files: 从文件对象对实例进行初始化
    rearrange_symbol: 将本实例中的数据的代码顺序按照给定的顺序排列(仅支持面板数据)
    drop_before_date: 将给定日期前(包含该日期)的数据剔除
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
        obj._data = pd_data.sort_index(ascending=True)
        return obj

    @classmethod
    def init_from_files(cls, data_file_objs, meta_data):
        '''
        使用文件对象对实例进行初始化

        Parameter
        ---------
        data_file_objs: iterable
            数据文件对象容器，元素为file object
        meta_data: dic
            元数据字典

        Return
        ------
        obj: DataWrapper
        '''
        obj = cls()

        def load_data(file_obj, symbols):
            # 从数据文件中加载数据
            # symbols非None时，data category必须为PANEL形式
            tmp_data = json.load(file_obj)
            if symbols is not None:  # PANEL数据
                for t in tmp_data:
                    tmp_data[t] = pd.Series(dict(zip(symbols, tmp_data[t])), index=symbols)
                tmp_data = pd.DataFrame(tmp_data).T.fillna(NaS)
            else:  # TIME SERIES数据
                tmp_data = pd.Series(tmp_data)
            tmp_data.index = pd.to_datetime(tmp_data.index)
            return cls.init_from_pd(tmp_data)

        symbols = meta_data['symbols'] if meta_data['data category'] == DataFormatCategory.PANEL else None
        data = [load_data(fp, symbols) for fp in data_file_objs]
        # 依照数据开始时间顺序进行逆序排列，此处假设各个文件的数据重叠的范围很小(内部实现中，应该是没有重叠)
        data = sorted(data, key=lambda x: x.start_time, reverse=True)

        def reduce_op(x, y):
            # 使用reduce对数据进行合成的函数
            x.update(y)  # 将y的数据插入到x的start_time前
            return x
        if len(data) > 1:   # 至少有两个实例
            out = reduce(reduce_op, data)
        else:
            out = data[0]
        return out

    def drop_before_date(self, date):
        '''
        将给定日期(包含该日期)的数据删去

        Parameter
        ---------
        date: datetime like
        '''
        date = pd.to_datetime(date)
        if date < self.start_time:  # 无重叠时间
            return
        data = self._data
        self._data = data.loc[data.index > date]

    def rearrange_symbol(self, symbol_order):
        '''
        对代码的顺序按照给定的顺序进行重新排列，若顺序相同则不做任何操作，仅PANEL数据支持该功能
        排列规则(与hdf5Engine中相同):
        优先按照给定的代码顺序进行排列，如果本实例中有多余的代码，再对这对于的代码按照升序排列

        symbol_order: iterable
            给定股票代码的顺序
        '''
        if self._data_category == DataFormatCategory.TIME_SERIES:
            raise NotImplementedError
        if self._data.columns.tolist() == list(symbol_order):   # 预先判断，避免过多操作
            return
        diff = sorted(self._data.columns.difference(symbol_order), reverse=False)
        new_order = symbol_order + diff
        self._data = self._data.reindex(columns=new_order)

    def update(self, other):
        '''
        使用另外一个实例对该实例的数据进行更新，如果有时间轴或者代码轴上的重叠，都
        以给定的参数为准
        若参数数据与本实例的数据时间轴上有重叠，要求两个实例的起始时间存在如下关系：
        other.start_time <= self.start_time <= other.end_time <= self.end_time
        即，将参数提供的数据插入到本实例的数据前

        Parameter
        ---------
        other: DataWrapper
        '''
        if self.data_category != other.data_category:
            raise ValueError('Incompatible data category, {dc} required, but {pdc} is provided'.
                             format(dc=self.data_category, pdc=other.data_category))
        if not (other.start_time <= self.start_time and other.end_time <= self.end_time):
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

    def split_data(self):
        '''
        将内部数据依照配置中的数据频率将数据分解为可以插入到文件中的单位

        Return
        ------
        out: dict
            格式为{file_name(without suffix): DataWrapper}
        '''
        tmp_data = self.data
        assigned_groups = tmp_data.groupby(date2filename)
        out = {}
        for fn in assigned_groups.groups:
            g_data = assigned_groups.get_group(fn)
            out[fn] = DataWrapper.init_from_pd(g_data)
        return out

    def to_jsonformat(self):
        '''
        将数据转换为JSON的形式

        Return
        ------
        out: dict
            若为面板数据则为{date: ['sample1', 'sample2', ...]}，若为时间序列数据则为{date: 'sample1', ...}
            返回的数据都为内置字典类型数据，键没有强制的顺序，在存储过程中需要注意
        Notes
        -----
        若为PANEL数据，所有导出的数据代码的顺序都以symbol_index为准，若需要对应的修改元数据中的universe则需要从
        symbol_index中读取
        '''
        if self._data_category == DataFormatCategory.PANEL:  # PANEL数据
            out = {}
            for date, data in self._data.iterrows():
                date_key = date.strftime(DB_CONFIG['db_time_format'])
                out[date_key] = data.tolist()
            return out
        else:   # TIME SERIES数据
            return self._data.to_dict()

    @property
    def start_time(self):
        return self._data.index[0]

    @property
    def end_time(self):
        return self._data.index[-1]

    @property
    def data_category(self):
        return self._data_category

    @property
    def symbol_index(self):
        if self._data_category == DataFormatCategory.TIME_SERIES:
            return None
        return self._data.columns.tolist()

    @property
    def data(self):
        return self._data.copy()

    def __len__(self):
        return len(self._data)



class JSONEngine(DBEngine):
    '''

    主引擎类
    提供以下接口:
    query: 类方法，依据给定的参数，从数据文件中查询相应的数据
    insert: 类方法，依据给定的参数，向数据文件中插入数据
    remove_data: 类方法，将数据库中给定的数据删除
    move_to: 类方法，将给定的数据移动到其他给定的位置

    Parameter
    ---------
    params: database.db.ParamsParser
    '''
    def __init__(self, params):
        self._properties = None
        self._params = params
        self._parse_path()

    def _parse_path(self):
        '''
        将相对路径解析为数据的绝对路径
        '''
        params = self._params
        rel_path = params.rel_path.replace('.', sep)
        params.set_absolute_path(join(params.main_path, rel_path))

    @classmethod
    def insert(cls, data, params):
        '''
        类方法，将给定的数据插入到数据文件中

        Parameter
        ---------
        data: pandas.DataFrame or pandas.Series

        params: database.db.ParamsParser

        Return
        ------
        result: boolean
        '''
        obj = cls(params)
        data = DataWrapper.init_from_pd(data)
        if not exists(obj._params.absolute_path):   # 首次插入数据，数据文件夹不存在
            makedirs(obj._params.absolute_path)
            new_metadata = {'start time': data.start_time.strftime(DB_CONFIG['db_time_format']),
                            'end time': data.end_time.strftime(DB_CONFIG['db_time_format']),
                            'data category': params.store_fmt[-1].name,
                            'time length': len(data),
                            'filled status': FilledStatus.FILLED.name}
            # 占位符，后续需要使用数据文件本地的元数据(若存在)，必须保证程序中存在metadata变量
            # 若后面出现与此占位符相关的错误，表示代码逻辑上有漏洞
            metadata = None
        else:   # 对现有数据的元数据进行更新
            obj._load_metadata()
            metadata = obj._properties
            data.drop_before_date(metadata['end time'])
            if len(data) == 0:   # 剔除后没有数据
                return False
            if data.data_category == DataFormatCategory.PANEL:  # 事先对代码顺序进行更新
                data.rearrange_symbol(metadata['symbols'])
            new_metadata = {'start time': metadata['start time'].strftime(DB_CONFIG['db_time_format']),
                            'end time': data.end_time.strftime(DB_CONFIG['db_time_format']),
                            'data category': metadata['data category'].name,
                            'time length': metadata['time length'] + len(data),
                            'filled status': FilledStatus.FILLED.name}
        if data.data_category == DataFormatCategory.PANEL:
            new_metadata['symbol length'] = len(data.symbol_index)
            new_metadata['symbols'] = data.symbol_index
        splited_data = data.split_data()
        for fn in sorted(splited_data.keys()):
            file_name = join(obj._params.absolute_path, fn + SUFFIX)
            tmp_data = splited_data[fn]
            if exists(file_name):   # 数据文件存在，则必然存在元数据文件
                with open(file_name, 'r') as f:
                    exist_data = DataWrapper.init_from_files([f], metadata)
                    tmp_data.update(exist_data)
            with open(file_name, 'w') as f:
                tobe_dumped = tmp_data.to_jsonformat()
                json.dump(tobe_dumped, f)
        obj._update_metadata(new_metadata)
        return True

    @classmethod
    def query(cls, params):
        '''
        类方法，从数据文件中查询给定的数据

        Parameter
        ---------
        params: database.db.ParamsParser
            start_time属性必须为非空，若end_time属性为None，则视作查询时点数据(仅支持PANEL)，反之则为查询时间序列数据

        Return
        ------
        out: pandas.DataFrame(PANEL) or pandas.Series(TIME_SERIES OR CROSS_SECTION)
        '''
        if params.start_time is None:
            raise ValueError('start_time property cannot be None!')
        obj = cls(params)
        metadata = obj._load_metadata()
        if metadata['data category'] == DataFormatCategory.TIME_SERIES and params.end_time is None: # 时间序列不能请求时点数据
            raise ValueError('Time series data cannot query PIT data!')
        file_names = obj._parse_filenames()
        opened_files = []
        try:
            for fn in self._parse_filenames():
                file_path = join(obj._params.absolute_path, fn + SUFFIX)
                if not exists(file_path):
                    continue
                opened_files.append(open(file_path, 'r'))
            data = DataWrapper.init_from_files(opened_files, metadata)
        finally:
            for fobj in opened_files:
                fobj.close()
        if params.end_time is None:
            try:
                out = data.loc[params.start_time]
            except KeyError:
                out = None
        else:
            mask = (data.index >= params.start_time) & (data.index <= params.end_time)
            out = data.loc[mask]
            if len(out) == 0:
                out = None
        return out

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
            rmtree(obj._params.absolute_path)
        except Exception as e:
            return False
        return True

    @classmethod
    def move_to(cls, src_params, dest_params):
        '''
        将给定的数据移动到另外其他给定位置
        '''
        dest_obj = cls(dest_params)
        if exists(dest_obj._params.absolute_path):
            raise ValueError('Cannot not move to an exsiting position!')
        src_obj = cls(src_params)
        try:
            move(src_obj._params.absolute_path, dest_obj._params.absolute_path)
        except Exception as e:
            return False
        return True

    def _parse_filenames(self):
        '''
        将参数中给定的时间区间或者时间点解析为对应的文件名列表，仅用于请求数据的情况

        Return
        ------
        out: list
            元素为数据的绝对路径(包含文件类型后缀)
        '''
        params = self._params
        if params.end_time is None:  # 表示请求的是横截面的数据
            return [date2filename(params.start_time)]
        else:
            return date2filenamelist(params.start_time, params.end_time)

    def _update_metadata(self, new_property):
        '''
        将元数据更新为给定的数据

        Parameter
        ---------
        new_property: dict
            格式如下，{'start time': st, 'end time': et, 'data category': dc, 'filled status': fs,
            'time length': tl, 'symbol length'(optional): sl, 'symbols'(optional): s}
        '''
        with open(join(self._params.absolute_path, METADATA_FILENAME), 'w') as f:
            json.dump(new_property, f)

    @staticmethod
    def _trans_metadata(json_metadata):
        '''
        将从JSON文件中读取出来的文件夹中的元数据文件进行转换

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
        return meta_data

    def _load_metadata(self):
        '''
        从文件中加载元数据，该函数假设元数据文件存在，若文件不存在将自动引发FileNotFoundError
        '''
        with open(join(self._params.absolute_path, METADATA_FILENAME), 'r') as f:
            self._properties = self._trans_metadata(json.load(f))

