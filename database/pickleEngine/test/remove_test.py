#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/14
"""

from database.db import ParamsParser
from database.pickleEngine.dbcore import PickleEngine
from database.const import DataClassification

db_path = r'C:\Users\c\Desktop\test\pickle_test'
params = ParamsParser.from_dict(db_path,
                                {'rel_path': 'rtest.test',
                                 'store_fmt': (DataClassification.UNSTRUCTURED, )})
data = list(range(1000))
PickleEngine.insert(data, params)
print(PickleEngine.query(params))
print(PickleEngine.remove_data(params))
