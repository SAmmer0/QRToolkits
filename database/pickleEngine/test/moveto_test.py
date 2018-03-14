#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/14
"""
from database.pickleEngine.dbcore import PickleEngine
from database.const import DataClassification
from database.db import ParamsParser

db_path = r'C:\Users\c\Desktop\test\pickle_test'
src_params = ParamsParser.from_dict(db_path,
                                {'rel_path': 'test',
                                 'store_fmt': (DataClassification.UNSTRUCTURED, )})
dest_params = ParamsParser.from_dict(db_path,
                                     {'rel_path': 'test.test',
                                      'store_fmt': (DataClassification.UNSTRUCTURED, )})
print(PickleEngine.move_to(src_params, dest_params))
