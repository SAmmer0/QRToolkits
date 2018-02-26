#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/26
"""

from os.path import exists, join
from shutil import rmtree

from fmanager import query
from database.jsonEngine.dbcore import JSONEngine
from database.const import DataFormatCategory, DataValueCategory, DataClassification
from database.db import ParamsParser


test_flag = 1  # 0表示测试无重叠时间的数据插入，1表示测试有重叠时间的数据插入
TEST_FACTOR = 'ZX_IND'
first_start = '2017-01-01'
first_end = '2017-08-01'
if test_flag == 0:
    second_start = '2017-08-02'
    second_end = '2018-02-01'
elif test_flag == 1:
    second_start = '2017-06-01'
    second_end = '2018-02-01'

first_sample = query(TEST_FACTOR, (first_start, first_end))
second_sample = query(TEST_FACTOR, (second_start, second_end))


db_path = r'C:\Users\c\Desktop\test'
data_path = 'json_test'
folder_path = join(db_path, data_path)
if exists(folder_path):
    rmtree(folder_path)

print(JSONEngine.insert(first_sample, ParamsParser.from_dict(db_path,
                                                       {'rel_path': data_path,
                                                        'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL)})))
print(JSONEngine.insert(second_sample, ParamsParser.from_dict(db_path,
                                                        {'rel_path': data_path,
                                                         'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL)})))
