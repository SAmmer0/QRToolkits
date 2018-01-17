#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 10:49:22
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from os import remove
from database.hdf5db.dbcore import *
from fmanager import query

TEST_FACTOR = 'CLOSE'
start_time = '2017-01-01'
end_time = '2018-01-15'

sample_df = query(TEST_FACTOR, (start_time, end_time))


db_path = r'C:\Users\c\Desktop\test\test.h5'
remove(db_path)
db = DBConnector.create_datafile(db_path, DataCate.PANEL)
db.insert(sample_df)
