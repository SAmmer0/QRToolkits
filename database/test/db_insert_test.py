#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""
from shutil import rmtree
from os.path import exists

from database.db import Database
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from fmanager import query

db_path = r'C:\Users\c\Desktop\test\db_test'
if exists(db_path):
    rmtree(db_path)
start_time = '2017-01-01'
end_time = '2018-02-01'

db = Database(db_path)

num_data = query('CLOSE', (start_time, end_time))
char_data = query('ZX_IND', (start_time, end_time))
third_data = query('BETA', (start_time, end_time))
unstruct_data = list(range(1000))

print(db.insert(num_data, 'num_test', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), 'float64'))
print(db.insert(char_data, 'char_test', (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL)))
print(db.insert(third_data, 'factor.beta', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), 'float64'))
print(db.insert(unstruct_data, 'unstruct_data', (DataClassification.UNSTRUCTURED, )))
