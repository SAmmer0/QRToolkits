# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 15:12:44 2018

@author: Hao Li

功能说明:
简易数据库，可以存储的数据类型为
结构化数据(DataClassification.STRUCTURED)
    数值型(DataValueCategory.NUMERIC)
        面板数据(DataFormatCategory.PANEL)
        时间序列数据(DataFormatCategory.TIME_SERIES)
    字符型(DataValueCategory.CHAR)
        面板数据(DataFormatCategory.PANEL)
        时间序列数据(DataFormatCategory.TIME_SERIES)
非结构化数据(DataClassification.UNSTRUCTURED)
暴露的接口:
insert, query, remove_data, move_to, find_data, find_collection, print_collections
"""
from database.db import Database
from database.const import DataClassification, DataFormatCategory, DataValueCategory
