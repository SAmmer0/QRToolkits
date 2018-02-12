#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 10:49:22
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
end_time = '2018-01-15'
new_end = '2018-02-01'

sample_df = query(TEST_FACTOR, (start_time, end_time))
new_data = query(TEST_FACTOR, (end_time, new_end))


db_path = r'C:\Users\c\Desktop\test'
# file_path = join(db_path, 'test.h5')
# if exists(file_path):
#     remove(file_path)
HDF5Engine.insert(new_data, ParamsParser.from_dict(db_path, {"rel_path": 'test', 
                                                     "store_fmt": (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                                                     "dtype": np_dtype('float64')}))
