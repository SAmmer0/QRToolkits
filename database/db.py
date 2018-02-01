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

from pandas import to_datetime

from database.const import DataClassification, DataValueCategory
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
    目前只支持两层分类，第一层为DataClassfication，第二层为DataValueCategory
    '''
    def __init__(self):
        self._rule = {DataClassification.STRUCTURED: [DataValueCategory.CHAR, DataValueCategory.NUMERIC],
                      DataClassification.UNSTRUCTURED: [None]}
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
            if cate not in rule[cate]:
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
        self._store_fmt = None
        self._abs_path = None
    
    @classmethod
    def from_dict(cls, db_path, params):
        '''
        使用字典类型的参数数据构造参数解析类
        
        Parameter
        ---------
        db_path: string
            数据库的绝对路径
        params: dict
            字典类型的参数，参数域包含['rel_path'(必须), 'start_time', 'end_time', 'store_fmt']
        
        Return
        ------
        obj: ParamsParser
        '''
        obj = cls()
        obj._db_path = db_path
        obj._start_time = params['start_time']
        if obj._start_time is not None:
            obj._start_time = to_datetime(obj._start_time)
        obj._end_time = params['start_time']
        if obj._end_time is not None:
            obj._end_time = to_datetime(obj._end_time)
        obj._abs_path = obj._parse_relpath(params['rel_path'])
        obj._store_fmt = params['store_fmt']
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
        

class Database(object):
    '''
    主数据库接口类，用于处理与外界的交互
    '''