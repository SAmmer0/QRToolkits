#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 13:06:50
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from os import remove
from os.path import exists, join

from numpy import dtype as np_dtype

from database.hdf5Engine.dbcore import HDF5Engine
from database.const import DataFormatCategory, DataValueCategory, DataClassification
from database.db import ParamsParser
from fmanager import query

TEST_FACTOR = 'CLOSE'
start_time = '2017-01-01'
end_time = '2017-12-30'
new_end = '2018-01-15'

second_insert_test = True
sample_series = query(TEST_FACTOR, (start_time, end_time)).iloc[:, 0]
new_sample_series = query(TEST_FACTOR, (start_time, new_end)).iloc[:, 0]

db_path = r'C:\Users\c\Desktop\test'
if not second_insert_test:
    file_path = join(db_path, 'test_series.h5')
    if exists(file_path):
        remove(file_path)
        HDF5Engine.insert(sample_series, ParamsParser.from_dict(db_path, {"rel_path": 'test_series', 
                                                                 "store_fmt": (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES),
                                                         "dtype": np_dtype('float64')}))
else:
    HDF5Engine.insert(new_sample_series, ParamsParser.from_dict(db_path, {"rel_path": 'test_series', 
                                                                 "store_fmt": (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.TIME_SERIES),
                                                         "dtype": np_dtype('float64')}))
