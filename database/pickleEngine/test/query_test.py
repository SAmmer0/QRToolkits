#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/14
"""

from database.db import ParamsParser
from database.const import DataClassification
from database.pickleEngine.dbcore import PickleEngine

db_path = r'C:\Users\c\Desktop\test\pickle_test'

params = ParamsParser.from_dict(db_path, {'rel_path': 'test',
                                          'store_fmt': (DataClassification.UNSTRUCTURED, )})

print(PickleEngine.query(params))
