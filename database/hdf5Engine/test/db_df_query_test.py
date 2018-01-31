#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 14:51:29
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$

from database.hdf5db.dbcore import *

start_time = '2017-03-01'
end_time = '2017-12-30'

db_path = r'C:\Users\c\Desktop\test\test.h5'
db = HDF5Engine.init_from_file(db_path)
data = db.query(start_time, end_time)
