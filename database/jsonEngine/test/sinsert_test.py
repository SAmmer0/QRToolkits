#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""
from shutil import rmtree
from os.path import exists, join

from fmanager import query
from database.db import ParamsParser
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from database.jsonEngine.dbcore import JSONEngine

OVERLAP_INSERT_FLAG = False   # 插入的数据是否有重叠
TEST_SECOND_FLAG = True    # 是否进行第二次插入

first_start = '2017-01-01'
first_end = '2017-06-01'

if OVERLAP_INSERT_FLAG:
    second_start = '2017-04-01'
    second_end = '2018-02-01'
else:
    second_start = '2017-06-02'
    second_end = '2018-02-01'

first_sample = query('ZX_IND', (first_start, first_end)).iloc[:, 0]
second_sample = query('ZX_IND', (second_start, second_end)).iloc[:, 0]

db_path = r'C:\Users\c\Desktop\test'
json_db = 'sjson_test'
folder_path = join(db_path, json_db)
if exists(folder_path):
    rmtree(folder_path)

print(JSONEngine.insert(first_sample, ParamsParser.from_dict(db_path,
                                                             {'rel_path': json_db,
                                                              'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES)})))
if TEST_SECOND_FLAG:
    print(JSONEngine.insert(second_sample, ParamsParser.from_dict(db_path,
                                                                 {'rel_path': json_db,
                                                                  'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES)})))
