#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""
import os
from shutil import rmtree

from numpy import all as np_all
from numpy import isclose as np_isclose

from database.db import Database
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from fmanager import query

db_path = r'C:\Users\c\Desktop\test\db_test'
start_time = '2017-01-01'
end_time = '2018-02-01'

if os.path.exists(db_path):
    rmtree(db_path)
    rmtree(r'C:\Users\c\Documents\DatabaseMetadata')

db = Database(db_path)

num_data = query('CLOSE', (start_time, end_time))
char_data = query('ZX_IND', (start_time, end_time))
#
db.insert(num_data, 'num_test', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL), 'float64')
db.insert(char_data, 'char_test', (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL))
#

print(db.move_to('num_test', 'num.num_dest', (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL)))
print(db.move_to('char_test', 'char.char_dest', (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL)))

