# -*- encoding: utf-8
'''
主数据库，用于管理和调用其他的数据库引擎

包含以下几个功能：
Database: 主数据库类
StoreFormat: 数据存储类型
ParamsParser: 通用参数类
DataNode: 数据文件结构树类
'''
import enum
import os.path as os_path
from os import remove as os_remove
from os import sep, makedirs
import warnings
import json
from collections import deque
import logging

from pandas import to_datetime
from numpy import dtype as np_dtype

from database.const import (DataClassification, DataValueCategory, DataFormatCategory,
                            ENCODING, CONFIG_PATH, REL_PATH_SEP)
from database.hdf5Engine.dbcore import HDF5Engine
from database.jsonEngine.dbcore import JSONEngine
from database.pickleEngine.dbcore import PickleEngine
from database.jsonEngine.const import SUFFIX as JSON_SUFFIX
from qrtutils import parse_config
from database.utils import set_db_logger
# ----------------------------------------------------------------------------------------------
# 全局预处理
# 设置日志
logger = logging.getLogger(set_db_logger())

# ----------------------------------------------------------------------------------------------
# 函数
def strs2StoreFormat(t):
    '''
    将字符串元组解析成为StoreFormat
    主要是为了避免日后对StoreFormat的格式进行拓展导致需要修改的地方太多

    Parameter
    ---------
    t: tuple
        按照给定顺序设置的字符串类型元组

    Return
    ------
    out: StoreFormat
    '''
    if len(t) > 3:
        raise NotImplementedError
    fmt = []
    formater = [DataClassification, DataValueCategory, DataFormatCategory]
    for fmter, desc in zip(formater, t):
        fmt.append(fmter[desc])
    return StoreFormat.from_iterable(fmt)


# ----------------------------------------------------------------------------------------------
# 类
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
                      DataValueCategory.NUMERIC: [DataFormatCategory.PANEL, DataFormatCategory.TIME_SERIES],
                      None: [None]}
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
                continue
            if cate not in rule[last_cate]:
                return False
            last_cate = cate
        return True

    def to_strtuple(self):
        '''
        将各个分类详情对象转化为字符串形式

        Return
        ------
        out: tuple
            每个元素为按照顺序排列的分类对象的字符串形式
        '''
        out = []
        for c in self._data:
            out.append(c.name)
        return out

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
        self._main_path = None
        self._start_time = None
        self._end_time = None
        self._engine_map_rule = {(DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL): HDF5Engine,
                                 (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES): HDF5Engine,
                                 (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL): JSONEngine,
                                 (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES): JSONEngine,
                                 (DataClassification.UNSTRUCTURED, ): PickleEngine}
        # 参数组合校验字典，键为(start_time is None, end_time is None)，值为对应的存储类型
        self._validation_rule = {(False, False): StoreFormat.from_iterable((DataClassification.STRUCTURED, )),
                                 (True, False): StoreFormat.from_iterable((DataClassification.STRUCTURED,)),
                                 (False, True): StoreFormat.from_iterable((DataClassification.STRUCTURED,)),
                                 (True, True): StoreFormat.from_iterable((DataClassification.UNSTRUCTURED,))}
        self._store_fmt = None
        self._rel_path = None
        self._absolute_path = None
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
            字典类型的参数，参数域包含['rel_path'(必须)(string), 'start_time'(datetime),
            'end_time'(datetime), 'store_fmt'(StoreFormat), 'dtype'(numpy.dtype)]

        Return
        ------
        obj: ParamsParser
        '''
        obj = cls()
        obj._main_path = db_path
        obj._start_time = params.get('start_time', None)
        if obj._start_time is not None:
            obj._start_time = to_datetime(obj._start_time)
        obj._end_time = params.get('end_time', None)
        if obj._end_time is not None:
            obj._end_time = to_datetime(obj._end_time)
        obj._rel_path = params['rel_path']
        obj._store_fmt = params.get('store_fmt', None)
        if obj._store_fmt is not None:
            obj._store_fmt = StoreFormat.from_iterable(obj._store_fmt)
        obj._dtype = params.get('dtype', None)
        if obj._dtype is not None:
            obj._dtype = np_dtype(obj._dtype)
        if not obj.store_fmt.validate():
            raise ValueError("Invalid parameter group!")
        return obj

    def get_engine(self):
        '''
        获取对应的数据库引擎
        '''
        return self._engine_map_rule[self._store_fmt.data]

    def set_absolute_path(self, abs_path):
        '''
        设置数据的绝对路径，路径的格式由设置的数据引擎规定，该方法仅由数据引擎调用，与该方法配对
        的有absolute_path只读属性，用于获取设置的absolute_path，将操作设置为方法是为了强调该方法仅
        能由数据引擎使用设置成descriptor可能会误导

        Parameter
        ---------
        abs_path: string
            由数据引擎自行解析的绝对路径
        '''
        self._absolute_path = abs_path


    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def main_path(self):
        return self._main_path

    @property
    def rel_path(self):
        return self._rel_path

    @property
    def absolute_path(self):
        return self._absolute_path

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
    find_collection: 查找给定名称的数据集
    find_data: 查找给定名称的数据
    print_collections: 打印当前数据库下数据组织结构

    Parameter
    ---------
    db_path: string
        数据的存储路径，以系统分隔符进行分割，分割后的最后一个名称被确定为数据库名称
        在设置数据库名称时，不要出现重名，即数据库的路径不同，但是名称相同的现象，这种行为会导致数据库管理的混乱
    '''
    def __init__(self, db_path):
        self._main_path = db_path
        self._data_tree_root = None
        self._db_name = self._main_path.split(sep)[-1]
        if not self._check_duplicate_db():
            raise ValueError('Database({}) already exists!'.format(self._db_name))
        self._load_meta()

    def _check_duplicate_db(self):
        '''
        检查是否有数据库出现重名现象，如果没有，则再检查该数据库是否注册，若没有则直接注册
        仅在数据库对象初始化时检查
        存储文件的格式为{db_name: main_path}

        Parameter
        ---------
        result: boolean
            True表示没有重复，False表示重复
        '''
        metadata_path = parse_config(CONFIG_PATH)['database_metadata_path']
        metadata_path = os_path.join(metadata_path, '$metadata.json')
        if not os_path.exists(metadata_path):   # 存储所有数据库信息的文件不存在，则创建，然后存储
            if not os_path.exists(os_path.dirname(metadata_path)):
                makedirs(os_path.dirname(metadata_path))
            metadata = {self._db_name: self._main_path}
            with open(metadata_path, 'w', encoding=ENCODING) as f:
                json.dump(metadata, f)
            return True
        with open(metadata_path, 'r', encoding=ENCODING) as f:
            metadata = json.load(f)
        if self._db_name not in metadata:   # 当前数据库未注册
            metadata[self._db_name] = self._main_path
            with open(metadata_path, 'w', encoding=ENCODING) as f:
                json.dump(metadata, f)
        else:
            if metadata[self._db_name] != self._main_path:
                return False
        return True


    def query(self, rel_path, store_fmt, start_time=None, end_time=None):
        '''
        查询数据接口

        Parameter
        ---------
        rel_path: string
            该数据在数据库中的相对路径，路径格式为db.sub_dir.sub_dir.sub_data
        store_fmt: StoreFormat or iterable
            数据存储格式分类，详情见模块文档
        start_time: datetime like
            数据开始时间(可选)，若请求的是面板数据的某个时间点的横截面数据，该参数不能为None，
            而end_time参数需要为None
            非结构化数据不需要设置该参数以及end_time参数
        end_time: datetime like
            数据结束时间(可选)

        Return
        ------
        out: pandas.Series, pandas.DataFrame or object

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.query('data1.data11.data112', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), '2017-01-01', '2018-01-01')
        >>> db.query('data2.data21', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES), '2017-01-01', '2018-01-01')
        '''
        params = ParamsParser.from_dict(self._main_path, {'rel_path': rel_path,
                                                        'store_fmt': store_fmt,
                                                        'start_time': start_time,
                                                        'end_time': end_time})
        # 时间参数校验规则，键为(start_time is None, end_time is None)，值为对应的数据结构分类
        validation_rule = {(True, True): DataClassification.UNSTRUCTURED,
                           (False, False): DataClassification.STRUCTURED,
                           (False, True): DataClassification.STRUCTURED,
                           (True, False): None}
        time_flag = (start_time is None, end_time is None)
        vclassification = validation_rule[time_flag]
        if vclassification is None or vclassification != params.store_fmt[0]:
            raise ValueError('Invalid parameter group in database query!')
        engine = params.get_engine()
        data = engine.query(params)
        return data

    def insert(self, data, rel_path, store_fmt, dtype=None):
        '''
        存储数据接口

        Parameter
        ---------
        data: pandas.Series, pandas.DataFrame or object
            需要插入的数据
        rel_path: string
            数据的相对路径
        store_fmt: StoreFormat or iterable
            数据存储格式分类，详情见模块文档
        dtype: numpy.dtype like, default None
            数据存储类型，目前仅数值型数据需要提供该参数
        Return
        ------
        issuccess: boolean
            是否成功插入数据，True表示成功

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.insert(data1, 'data1.data11.data112', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), 'float64')
        >>> db.insert(data2, 'data2.data21', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES), 'int32')
        '''
        params = ParamsParser.from_dict(self._main_path, {'rel_path': rel_path,
                                                        'store_fmt': store_fmt,
                                                        'dtype': dtype})
        engine = params.get_engine()
        issuccess = engine.insert(data, params)
        if issuccess:   # 数据成功插入，修改检查元数据是否需要修改，并采取相应操作
            if self._data_tree_root.has_offspring(rel_path) is None:
                self._data_tree_root.add_offspring(rel_path, params.store_fmt)
                self._dump_meta()
        else:
            logger.warn('[Operation=Database.insert, Info=\"Inserting data failed!(db={db_name}, rel_path={rel_path})\"]'.
                        format(db_name=self._db_name, rel_path=rel_path))
        return issuccess

    def remove_data(self, rel_path, store_fmt):
        '''
        将给定路径的数据删除

        Parameter
        ---------
        rel_path: string
            数据的相对路径
        store_fmt: StoreFormat
            数据存储方式分类，详情见模块文档

        Return
        ------
        issuccess: boolean

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.remove_data('data1.data11.data112', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES))
        '''
        params = ParamsParser.from_dict(self._main_path, {'rel_path': rel_path,
                                                        'store_fmt': store_fmt})
        engine = params.get_engine()
        issuccess = engine.remove_data(params)
        if issuccess:
            node = self._data_tree_root.has_offspring(rel_path)
            if node is None:
                raise ValueError('Node does not exist!')
            node.parent.delete_child(node.node_name)
            self._dump_meta()
        else:
            logger.warn('[Operation=Database.remove_data, Info=\"Removing data failed!(db={db_name}, rel_path={rel_path})\"]'.
                        format(db_name=self._db_name,rel_path=rel_path))
        return issuccess


    def move_to(self, source_rel_path, dest_rel_path, store_fmt):
        '''
        将数据有原路径移动到新的路径下

        source_rel_path: string
            原存储位置的相对路径
        dest_rel_path: string
            目标存储位置的相对路径
        store_fmt: StoreFormat
            数据存储方式分类，详情见模块文档

        Return
        ------
        issuccess: boolean

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.move_to('data1.data11.data112', 'data1.data12.data121', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES))
        '''
        src_params = ParamsParser.from_dict(self._main_path, {'rel_path': source_rel_path,
                                                            'store_fmt': store_fmt})
        dest_params = ParamsParser.from_dict(self._main_path, {'rel_path': dest_rel_path,
                                                             'store_fmt': store_fmt})
        if source_rel_path == dest_rel_path:
            logger.warn('[Operation=Database.move_to, Info=\"source_rel_path({}) and dest_rel_path are the same!\"]'.
                        format(source_rel_path))
            return False
        engine = src_params.get_engine()
        issuccess = engine.move_to(src_params, dest_params)
        if issuccess:
            # 更新元数据
            node = self._data_tree_root.has_offspring(source_rel_path)
            if node is None:
                raise ValueError('Node does not exist!(rel_path=\"{}\"'.format(source_rel_path))
            node.parent.delete_child(node.node_name)
            rel_path_split = dest_rel_path.split(REL_PATH_SEP)
            node.change_nodename(rel_path_split[-1])
            new_parent_path = REL_PATH_SEP.join(rel_path_split[:-1])
            new_parent_node = self._data_tree_root.has_offspring(new_parent_path)
            if new_parent_node is None:
                self._data_tree_root.add_offspring(new_parent_path)
                new_parent_node = self._data_tree_root.has_offspring(new_parent_path)
            new_parent_node.add_child(node)
            self._dump_meta()
        else:
            logger.warn('[Operation=Database.move_to, Info=\"Moving data failed!(db={db_name}, rel_path={rel_path})\"]'.
                        format(db_name=self._db_name, rel_path=source_rel_path))
        return issuccess

    def list_alldata(self):
        '''
        返回所有数据的相对路径

        Return
        ------
        out: list
            元素为每个数据的相对路径
        '''
        out = []
        def get_all(node):
            if node.is_leaf:
                out.append(self._trans_node_relpath(node.rel_path))
            else:
                for child in node.children:
                    get_all(child)
        get_all(self._data_tree_root)
        return out

    def find_data(self, name):
        '''
        查找给定名称的数据
        内部查询采用BFS算法

        Parameter
        ---------
        name: string
            数据的名称

        Return
        ------
        out: list
            元素为字典形式，格式为{'rel_path': rel_path, 'store_fmt': store_fmt}

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.find_data('data1.data11.data112')
        '''
        nodes = self._find(name, self._data_tree_root, self._precisely_match, True)
        out = [{'rel_path': self._trans_node_relpath(n.rel_path), 'store_fmt': n.store_fmt}
                for n in nodes]
        return out

    def find_collection(self, name):
        '''
        查找给定名称的数据集合，内部查询采用BFS算法

        Parameter
        ---------
        name: string
            数据集名称

        Return
        ------
        out: dict
            结果的格式为{collection_rel_path: [{'rel_path': rel_path, 'store_fmt': store_fmt}]}

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.find_collection('data1.data11')
        '''
        nodes = self._find(name, self._data_tree_root, self._precisely_match, False)

        def get_leaf_nodes(node):
            queue = deque()
            queue.append(node)
            result = []
            while len(queue) > 0:
                current_node = queue.pop()
                if current_node.is_leaf:
                    result.append({'rel_path': self._trans_node_relpath(current_node.rel_path),
                                   'store_fmt': current_node.store_fmt})
                else:
                    for child in current_node.children:
                        queue.append(child)
            return result

        out = {self._trans_node_relpath(n.rel_path): get_leaf_nodes(n) for n in nodes}
        return out

    def print_collections(self, name=None):
        '''
        查找给定的数据集，并答应数据集下所包含的所有数据的组织结构

        Parameter
        ---------
        name: string, default None
            数据集名称，None表示打印整个文件树

        Example
        -------
        >>> db = Database(r'some_path')
        >>> db.print_collections()
        '''
        if name is None:
            tobe_printed = [self._data_tree_root]
        else:
            tobe_printed = self._find(name, self._data_tree_root, self._precisely_match, False)
        for root in tobe_printed:
            root.print_node('')
            print('\n')


    def _get_metadata_filename(self):
        '''
        解析数据库的主路径，获取存储的元数据的名称，目前假设主路径(无论是文件型数据引擎还是商用数据库型数据引擎)
        主路径都的基本形式都是xxxx/(sys_sep)xxxx/xxxx，内部实现中都是以最后一级名称作为元数据文件的名称

        Return
        ------
        fn: string
        '''
        metadata_path = parse_config(CONFIG_PATH)['database_metadata_path']
        return os_path.join(metadata_path, self._db_name+JSON_SUFFIX)


    def _load_meta(self):
        '''
        加载该数据库的元数据，转化为数据文件树，若无法找到元数据文件，则直接建立根节点
        '''
        metadata_path = self._get_metadata_filename()
        try:
            with open(metadata_path, 'r', encoding=ENCODING) as f:
                meta_data = json.load(f)
            self._data_tree_root = DataNode.init_from_meta(meta_data)
        except FileNotFoundError:
            self._data_tree_root = DataNode(self._db_name)


    def _find(self, name, node, match_func, leaf_node):
        '''
        具体实现数据或者数据集合名查找的函数
        采用BFS算法遍历查找

        Parameter
        ---------
        name: string
            数据或者数据集合的名称
        node: DataNode
            查找的起始节点
        match_func: callable
            判断两个名字是否相匹配的函数，格式签名为match_func(name, node_name)->boolean
        leaf_node: boolean
            True表示查找叶子结点

        Return
        ------
        result: list
            DataNode列表，若未查找到，则为空列表
        '''
        result = []
        if not node.is_leaf:
            if not leaf_node:   # 查找中间节点
                if match_func(name, node.node_name):
                    result.append(node)
                    return result
            for child in node.children:
                tmp_res = self._find(name, child, match_func, leaf_node)
                result += tmp_res
            return result
        else:
            if leaf_node:
                if match_func(name, node.node_name):
                    result.append(node)
            return result


    @staticmethod
    def _precisely_match(name, node_name):
        '''
        精确查找函数，即只有两个字符串相等才行

        Parameter
        ---------
        name: string
        node_name: string

        Return
        ------
        result: boolean
        '''
        return name == node_name

    def _dump_meta(self):
        '''
        将更新后的结构写入到元数据中，仅在每次发生元数据更新后调用
        '''
        metadata = self._data_tree_root.to_dict()
        metadata_path = self._get_metadata_filename()
        with open(metadata_path, 'w', encoding=ENCODING) as f:
            json.dump(metadata, f)

    @staticmethod
    def _trans_node_relpath(node_rel_path):
        '''
        将数据节点的相对路径进行转换，去除开头的相对路径分隔符
        Parameter
        ---------
        node_rel_path: string
            节点相对路径

        Return
        ------
        out: string
        '''
        return node_rel_path[1:]


class DataNode(object):
    '''
    文件结构树节点类，用于标识数据库中各个数据文件(夹)之间的包含关系，其中根节点的parent为None

    Parameter
    ---------
    node_name: string
        当前节点名称，即数据文件的名称
    store_fmt: StoreFormat, default None
        仅叶子节点为非空
    '''
    def __init__(self, node_name, store_fmt=None):
        self._node_name = node_name
        self._store_fmt = store_fmt
        self._children = {}
        self._parent = None

    @classmethod
    def init_from_meta(cls, meta_data):
        '''
        从存储有数据文件结构的文件中对数进行初始化，目前使用BFS算法

        Parameter
        ---------
        meta_data: dict
            字典的形式如下:
            {
                "node_name": db_name,
                "children": [
                    {"node_name": folder1,
                    "children": [{"node_name": data11, "store_fmt": store_fmt}, {}, ...]},
                    {"node_name": folder2,
                    "children": [{}, {}, ...]},
                    ...
                ]
            }
            只有叶子节点没有children键

        Return
        ------
        obj: DataNode
            存储有当前数据的节点
        '''
        if 'children' in meta_data:   # 表示当前不是叶子节点
            obj = cls(meta_data['node_name'])
            for child_meta in meta_data['children']:
                child_obj = cls.init_from_meta(child_meta)
                obj.add_child(child_obj)
            return obj
        else:   # 叶子节点
            obj = cls(meta_data['node_name'], strs2StoreFormat(meta_data['store_fmt']))
            return obj


    def add_child(self, child):
        '''
        向该节点添加直接连接的子节点，并且将子节点的母节点设置为当前节点

        Parameter
        ---------
        child: DataNode
        '''
        if self.has_child(child):
            raise ValueError('Current node({}) tries to overlap a child node!'.format(self._node_name))
        child._parent = self
        self._children[child.node_name] = child

    def delete_child(self, child_name):
        '''
        将给定名称的(直接连接的)节点从该节点的子节点中删除，同时也将子节点的母节点重置为None

        Parameter
        ---------
        child_name: string
            子节点的节点名称，即node_name

        Return
        ------
        result: boolean
        '''
        if not self.has_child(child_name):
            logger.warn("[Operation=DataNode.delete_child, Info=\"Current node({cn}) tries to delete an unexisting child({cl})!\"]".
                        format(cn=self._node_name, cl=child_name))
            return False
        child = self._children[child_name]
        del self._children[child_name]
        child._parent = None
        if len(self._children) == 0 and not self.is_root:    # 表明当前中间节点没有任何子节点
            self._parent.delete_child(self._node_name)

    def has_child(self, child):
        '''
        判断给定的节点名称是否是当前节点的直接连接的子节点

        Parameter
        ---------
        child: string
            子节点名称

        Return
        ------
        result: boolean
        '''
        return child in self._children

    def to_dict(self):
        '''
        将该节点的数据以字典的形式导出

        Return
        ------
        out: dict
            字典的形式如下:
            {
                "node_name": db_name,
                "children": [
                    {"node_name": folder1,
                    "children": [{"node_name": data11, "store_fmt": store_fmt}, {}, ...]},
                    {"node_name": folder2,
                    "children": [{}, {}, ...]},
                    ...
                ]
            }
        '''
        out = {}
        out['node_name'] = self._node_name
        if self.is_leaf:
            out['store_fmt'] = list(self._store_fmt.to_strtuple())
            return out
        else:
            children_list = []
            for child in self._children.values():
                children_list.append(child.to_dict())
            out['children'] = children_list
            return out

    def has_offspring(self, rel_path):
        '''
        判断是否具有某个相对路径确定的后代节点

        Parameter
        ---------
        rel_path: string
            以点分割的相对路径，例如data1.data2.data3

        Return
        ------
        node: DataNode
            如果找到了该路径下的节点，则返回该节点，反之，返回None
        '''
        offsprings = rel_path.split(REL_PATH_SEP)
        child = self._children.get(offsprings[0], None)
        if child is None:   # 没有该后代
            return None
        if len(offsprings) == 1:    # 最后一个查询节点
            return child
        else:
            return child.has_offspring(REL_PATH_SEP.join(offsprings[1:]))

    def add_offspring(self, rel_path, store_fmt=None):
        '''
        依据给定的相对路径递归地向数据树中添加后代

        Parameter
        ---------
        rel_path: string
            以点分割的相对路径，例如data1.data2.data3
        store_fmt: StoreFormat, default None
            非None表示该相对路径是叶子节点，仅在最后的叶子结点上添加store_fmt数据

        Return
        ------
        node: DataNode
            添加后的节点对象

        Notes
        -----
        该函数并不判断是否存在给定相对路径是否存在(性能要求)，因此，在每次使用该方法添加后代前，需要自行
        调用has_offspring来判断该相对路径的后代是否存在，否则如果发生覆盖会引发ValueError
        '''
        offsprings = rel_path.split(REL_PATH_SEP)
        child = self._children.get(offsprings[0], None)
        if child is None:   # 当前节点不存在
            if len(offsprings) == 1:    # 表示已经到了最终的节点
                child = DataNode(offsprings[0], store_fmt)
            else:
                child = DataNode(offsprings[0])
                child.add_offspring(REL_PATH_SEP.join(offsprings[1:]), store_fmt)
            self.add_child(child)
        else:
            if len(offsprings) > 1:
                child.add_offspring(REL_PATH_SEP.join(offsprings[1:]), store_fmt)
            else:
                raise ValueError('Current node({}) tries to overlap an existing node!'.format(self._node_name))

    def change_nodename(self, new_name):
        '''
        改变节点的名字，对应修改母节点中的相关信息

        Parameter
        ---------
        new_name: string
        '''
        if new_name == self._node_name:  # 名称相同，则不变
            return
        parent = self._parent
        if not self.is_root:
            parent.delete_child(self._node_name)
            self._node_name = new_name
            parent.add_child(self)
        else:
            self._node_name = new_name

    def print_node(self, indention):
        '''
        按照DFS算法顺序递归打印该节点及所有子节点

        Parameter
        ---------
        indention: string
            上一级的缩进
        '''
        this_level_indention = indention + '|####'
        print(this_level_indention + ' ' + self._node_name)
        for child in self.children:
            child.print_node(this_level_indention)

    @property
    def node_name(self):
        return self._node_name

    @property
    def children(self):
        return self._children.values()

    @property
    def parent(self):
        return self._parent

    @property
    def store_fmt(self):
        return self._store_fmt

    @property
    def is_leaf(self):
        return self._store_fmt is not None

    @property
    def is_root(self):
        return self._parent is None

    @property
    def rel_path(self):
        if self.is_root:    # 根节点为数据库名，没有相对路径
            return ''
        else:
            return self._parent.rel_path + REL_PATH_SEP + self._node_name

