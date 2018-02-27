#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""
from shutil import rmtree
from os.path import join, exists
from time import time

import numpy as np

from database.jsonEngine.dbcore import JSONEngine
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from database.db import ParamsParser
from fmanager import query

sample_start_time = '2017-01-01'
sample_end_time = '2018-02-01'

query_start_time = '2017-05-01'
query_end_time = '2017-12-04'

sample_data = query('ZX_IND', (sample_start_time, sample_end_time)).iloc[:, 0]

db_path = r'C:\Users\c\Desktop\test'
json_db = 'series_query_test'
folder_path = join(db_path, json_db)
if exists(folder_path):
    rmtree(folder_path)
JSONEngine.insert(sample_data, ParamsParser.from_dict(db_path,
                                                      {'rel_path': json_db,
                                                       'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES)}))

stime = time()
old_db_data = query('ZX_IND', (query_start_time, query_end_time)).iloc[:, 0]
etime = time()
print(etime-stime)
stime = time()
jsondb_data = JSONEngine.query(ParamsParser.from_dict(db_path,
                                                      {'rel_path': json_db,
                                                       'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES),
                                                       'start_time': query_start_time,
                                                       'end_time': query_end_time}))
etime = time()
print(etime-stime)

print(np.all(old_db_data==jsondb_data))
