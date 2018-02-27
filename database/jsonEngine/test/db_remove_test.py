#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/2/27
"""

from database.const import DataClassification, DataFormatCategory, DataValueCategory
from database.jsonEngine.dbcore import JSONEngine
from database.db import ParamsParser

db_path = r'C:\Users\c\Desktop\test'
json_db = 'sjson_test'

print(JSONEngine.remove_data(ParamsParser.from_dict(db_path, {'rel_path': json_db,
                                                              'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.TIME_SERIES)})))
