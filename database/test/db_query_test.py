#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""
from numpy import all as np_all
from numpy import isclose as np_isclose

from database.db import Database
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from fmanager import query

db_path = r'C:\Users\c\Desktop\test\db_test'
start_time = '2017-01-01'
end_time = '2018-02-01'

db = Database(db_path)

num_data = query('CLOSE', (start_time, end_time))
char_data = query('ZX_IND', (start_time, end_time))
unstruct_data = list(range(1000))

db.insert(num_data, 'num_test', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), 'float64')
db.insert(char_data, 'char_test', (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL))
db.insert(unstruct_data, 'unstruct_data.test', (DataClassification.UNSTRUCTURED, ))

query_start = '2017-05-01'
query_end = '2017-12-06'

queryed_num_data = db.query('num_test', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                            query_start, query_end)
old_num_data = query('CLOSE', (query_start, query_end))
print(np_all(np_isclose(queryed_num_data.fillna(0), old_num_data.fillna(0))))

queryed_char_data = db.query('char_test', (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL),
                             query_start, query_end)
old_char_data = query('ZX_IND', (query_start, query_end))
print(np_all(queryed_char_data==old_char_data))

print(db.query('unstruct_data.test', (DataClassification.UNSTRUCTURED, )))
