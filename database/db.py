# -*- encoding: utf-8
'''
主数据库，用于管理和调用其他的数据库引擎

包含以下几个功能：
DBEngine: 数据库引擎抽象基类
Database: 主数据库
StoreFormat: 数据存储类型
ParamsParser: 通用参数类
'''
import abc
import enum

from database.const import DataClassification, DataValueCategory

class DBEngine(object, metaclass=abc.ABCMeta):
    '''
    数据引擎虚基类，定义了query和insert两个数据处理接口
    '''
    @abc.abstractclassmethod
    def query(self, *args, **kwargs):
        '''
        使用数据引擎从数据文件或者数据库中获取数据
        '''
        pass
    
    @abc.abstractclassmethod
    def insert(self, *args, **kwargs):
        '''
        通过数据引擎将数据插入到数据文件或者数据库中，返回布尔值显示是否插入成功
        '''
        pass
    

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
    
    def __hash__(self):
        return hash(self._data)
    
    @property
    def data(self):
        return self._data
    
    @property
    def level(self):
        return len(self._data)
    

class ParamsParser(object):
    '''
    参数解析类
    '''