# -*- encoding: utf-8
'''
主数据库，用于管理和调用其他的数据库引擎

包含以下几个功能：
DBEngine: 数据库引擎抽象基类
Database: 主数据库
StoreFormat: 数据存储类型
ParamsParser: 通用参数类
'''
import enum
import os.path as path
import os.remove as os_remove

from pandas import to_datetime
from numpy import dtype as np_dtype

from database.const import DataClassification, DataValueCategory, DataFormatCategory
from database.hdf5Engine.dbcore import HDF5Engine

class StoreFormat(object):
    '''
    数据存储格式的分类
    提供方法如下:
    from_iterable: 使用类方法构造对象
    validate: 返回当前分类分类是否合理
    提供属性如下: 
    level: int，分类的层级
    data: tuple，分类详情，所有分类必须是enum.Enum的子类
    
    Notes
    -----
    目前只支持三层分类，第一层为DataClassfication，第二层为DataValueCategory，第三层为DataFormatCategory
    '''
    def __init__(self):
        self._rule = {DataClassification.STRUCTURED: [DataValueCategory.CHAR, DataValueCategory.NUMERIC],
                      DataClassification.UNSTRUCTURED: [None],
                      DataValueCategory.CHAR: [DataFormatCategory.PANEL, DataFormatCategory.TIME_SERIES],
                      DataValueCategory.NUMERIC: [DataFormatCategory.PANEL, DataFormatCategory.TIME_SERIES]}
        self._data = None
        
    @classmethod
    def from_iterable(cls, iterable):
        '''
        使用可迭代对象构造对象
        
        Parameter
        ---------
        iterable: iterable
            可迭代对象，每个元素为enum.Enum的子类
        
        Return
        ------
        obj: StoreFormat
        '''
        obj = cls()
        data = list(iterable)
        for classfication in data:
            if not isinstance(classfication, enum.Enum):
                raise TypeError('Elements must bet the subtype of enum.Enum')
        obj._data = tuple(data)
        return obj
    
    def validate(self):
        '''
        验证当前分类数据是否符合规则
        
        Return 
        ------
        validated: boolean
            若符合规则，返回True，反之返回False
        '''
        last_cate = None
        rule = self._rule
        for cate in self._data:
            if last_cate is None:
                last_cate = cate
                pass
            if cate not in rule[last_cate]:
                return False
            last_cate = cate
        return True
    
    def __eq__(self, other):
        for cate1, cate2 in zip(self, other):
            if cate1 != cate2:
                return False
            return True
    
    def __iter__(self):
        return iter(self._data)
    
    @property
    def data(self):
        return self._data
    
    @property
    def level(self):
        return len(self._data)
    
    def __getitem__(self, level):
        '''
        获取给定level的分类值
        
        Parameter
        ---------
        level: int
            所需要获取的分类的等级，0表示第一级，1表示第二级，...，以此类推
            
        Return
        ------
        out: enum.Enum
        '''
        return self._data[level]

class ParamsParser(object):
    '''
    参数解析类，用于对传入数据库引擎的数据进行包装
    
    该类提供一下方法: 
    from_dict: 类方法，从参数字典中对对象进行初始化
    get_engine: 通过定义的规则获取对应的数据引擎
    parse_relpath: 将相对路径解析为绝对路径
    '''
    def __init__(self):
        self._db_path = None
        self._start_time = None
        self._end_time = None
        self._engine_map_rule = {(DataClassification.STRUCTURED, DataValueCategory.NUMERIC): HDF5Engine}
        # 参数组合校验字典，键为(start_time is None, end_time is None)，值为对应的存储类型
        self._validation_rule = {(False, False): StoreFormat.from_iterable((DataClassification.STRUCTURED, )),
                                 (True, False): StoreFormat.from_iterable((DataClassification.STRUCTURED,)),
                                 (False, True): StoreFormat.from_iterable((DataClassification.STRUCTURED,)),
                                 (True, True): StoreFormat.from_iterable((DataClassification.UNSTRUCTURED,))}
        self._store_fmt = None
        self._abs_path = None
        self._dtype = None
    
    @classmethod
    def from_dict(cls, db_path, params):
        '''
        使用字典类型的参数数据构造参数解析类
        
        Parameter
        ---------
        db_path: string
            数据库的绝对路径
        params: dict
            字典类型的参数，参数域包含['rel_path'(必须)(string), 'start_time'(datetime), 'end_time'(datetime), 'store_fmt'(StoreFormat), 'dtype'(numpy.dtype)]
        
        Return
        ------
        obj: ParamsParser
        '''
        obj = cls()
        obj._db_path = db_path
        obj._start_time = params.get('start_time', None)
        if obj._start_time is not None:
            obj._start_time = to_datetime(obj._start_time)
        obj._end_time = params.get('end_time', None)
        if obj._end_time is not None:
            obj._end_time = to_datetime(obj._end_time)
        obj._abs_path = obj._parse_relpath(params['rel_path'])
        obj._store_fmt = params.get('store_fmt', None)
        obj._dtype = params.get('dtype', None)
        if obj._dtype is not None:
            obj._dtype = np_dtype(obj._dtype)
        if not obj._check_parameters():
            raise ValueError("Invalid parameter group!")
        return obj
    
    def get_engine(self):
        '''
        获取对应的数据库引擎
        '''
        return self._engine_map_rule[self._store_fmt.data]
    
    def _parse_relpath(self, rel_path):
        '''
        计算绝对路径
        
        Parameter
        ---------
        rel_path: string
           相对路径的格式为dir.dir.filename
        
        Return
        ------
        abs_path: string
            绝对路径
        '''
        from os import sep
        from os.path import join
        rel_path = rel_path.replace('.', sep)
        return join(self._db_path, rel_path)

    def _check_parameters(self):
        '''
        检查参数组合的合法性，包含数据类型组合的合法性以及时间参数与对应的引擎组合的合法性
        '''
        if self.store_fmt is None:  # 此时没有提供store_fmt参数，因此不需要检查
            return True
        if not self.store_fmt.validate():
            return False
        time_tuple = (self.start_time is None, self.end_time is None)
        dclassification = self._validation_rule[time_tuple]
        return dclassification[0] == self.store_fmt[0]
        

    @property
    def start_time(self):
        return self._start_time
    
    @property
    def end_time(self):
        return self._end_time
    
    @property
    def absolute_path(self):
        return self._abs_path
    
    @property
    def store_fmt(self):
        return self._store_fmt
    
    @property
    def dtype(self):
        return self._dtype
        

class Database(object):
    '''
    主数据库接口类，用于处理与外界的交互
    目前支持以下方法: 
    query: 获取请求的数据
    insert: 将数据存储到本地
    remove_data: 将给定路径的数据删除
    move_to: 将给定的数据移动到其他位置
    
    Parameter
    ---------
    db_path: string
        数据的存储路径
    '''
    def __init__(self, db_path):
        self._db_path = db_path
    
    def query(self, rel_path, store_fmt, start_time=None, end_time=None):
        '''
        查询数据接口
        
        Parameter
        ---------
        rel_path: string
            该数据在数据库中的相对路径，路径格式为db.sub_dir.sub_dir.sub_data
        store_fmt: StoreFormat or iterable
            数据存储格式分类
        start_time: datetime like
            数据开始时间(可选)
        end_time: datetime like
            数据结束时间(可选)
        
        Return
        ------
        out: pandas.Series, pandas.DataFrame or object
        '''
        params = ParamsParser.from_dict(self._db_path, {'rel_path': rel_path, 
                                                        'store_fmt': store_fmt,
                                                        'start_time': start_time, 
                                                        'end_time': end_time})
        engine = params.get_engine()
        data = engine.query(params)
        return data
    
    def insert(self, data, rel_path, store_fmt, dtype):
        '''
        存储数据接口
        
        Parameter
        ---------
        data: pandas.Series, pandas.DataFrame or object
            需要插入的数据
        rel_path: string
            数据的相对路径
        store_fmt: StoreFormat or iterable
            数据存储格式分类
        dtype: numpy.dtype like
            数据存储类型
        Return
        ------
        issuccess: boolean
            是否成功插入数据，True表示成功
        '''
        params = ParamsParser.from_dict(self._db_path, {'rel_path': rel_path,
                                                        'store_fmt': store_fmt,
                                                        'dtype': dtype})
        engine = params.get_engine()
        issuccess = engine.insert(data, params)
        return issuccess
    
    def remove_data(self, rel_path, store_fmt):
        '''
        将给定路径的数据删除
        
        Parameter
        ---------
        rel_path: string
            数据的相对路径
        store_fmt: StoreFormat
            数据存储方式分类
        
        Return
        ------
        issuccess: boolean
        '''
        params = ParamsParser.from_dict(self._db_path, {'rel_path': rel_path,
                                                        'store_fmt': store_fmt})
        engine = params.get_engine()
        issuccess = engine.remove_data(params)
        return issuccess
        
    
    def move_to(self, source_rel_path, dest_rel_path, store_fmt):
        '''
        将数据有原路径移动到新的路径下
        
        source_rel_path: string
            原存储位置的相对路径
        dest_rel_path: string
            目标存储位置的相对路径
        store_fmt: StoreFormat
            数据存储方式分类
        
        Return
        ------
        issuccess: boolean
        '''
        src_params = ParamsParser.from_dict(self._db_path, {'rel_path': source_rel_path, 
                                                            'store_fmt': store_fmt})
        dest_params = ParamsParser.from_dict(self._db_path, {'rel_path': dest_rel_path,
                                                             'store_fmt': store_fmt})
        engine = src_params.get_engine()
        issuccess = engine.move_to(src_params, dest_params)
        return issuccess
        
