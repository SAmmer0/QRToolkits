#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 13:06:50
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from os import remove

from database.hdf5db.dbcore import *
from fmanager import query

TEST_FACTOR = 'CLOSE'
start_time = '2017-01-01'
end_time = '2017-12-30'
new_end = '2018-01-15'

sample_series = query(TEST_FACTOR, (start_time, end_time)).iloc[:, 0]
new_sample_series = query(TEST_FACTOR, (start_time, new_end)).iloc[:, 0]

db_path = r'C:\Users\c\Desktop\test\test_series.h5'
# remove(db_path)
# db = DBConnector.create_datafile(db_path, DataCate.TIME_SERIES)
db = DBConnector.init_from_file(db_path)
# db.insert(sample_series)
db.insert(new_sample_series)
