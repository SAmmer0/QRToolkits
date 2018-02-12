#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 15:13:21
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from random import shuffle, seed
from time import sleep
from os import remove
from os.path import join

import numpy as np

from numpy import dtype as np_dtype

from database.hdf5Engine.dbcore import HDF5Engine
from database.const import DataFormatCategory, DataValueCategory, DataClassification
from database.db import ParamsParser
from fmanager import query

start_time = '2017-01-01'
first_end_time = '2017-06-01'
second_end_time = '2017-12-30'
test_factor = 'CLOSE'
db_path = r'C:\Users\c\Desktop\test'
rel_path = 'test_reshape'
initial_size = 1000

seed(1)

enable_second = False

def first_insert():
    data = query(test_factor, (start_time, first_end_time)).iloc[:, :initial_size]
    columns = list(data.columns)
    shuffle(columns)
    data = data.loc[:, columns]
    HDF5Engine.insert(data, ParamsParser.from_dict(db_path, {'rel_path': rel_path, 
                                                    'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                                                    'dtype': np_dtype('float64')}))


def second_insert():
    data = query(test_factor, (start_time, second_end_time))
    HDF5Engine.insert(data, ParamsParser.from_dict(db_path, {'rel_path': rel_path, 
                                                    'store_fmt': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                                                    'dtype': np_dtype('float64')}))


def query_compare():
    old_db_data = query(test_factor, (start_time, second_end_time))
    threshold_loc = old_db_data.index.get_loc(first_end_time) + 1
    
    new_db_data = HDF5Engine.query(ParamsParser.from_dict(db_path, {"rel_path": 'test', 
                                                  'start_time': start_time,
                                                  'end_time': second_end_time,
                                                  "store_fmt": (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL)}))
    old_db_data = old_db_data.fillna(0)
    new_db_data = new_db_data.fillna(0)
    columns1 = new_db_data.columns[:initial_size]
    columns2 = new_db_data.columns
    is_close1 = np.isclose(old_db_data.ix[:threshold_loc, columns1],
                           new_db_data.ix[:threshold_loc, columns1])
    is_close2 = np.isclose(old_db_data.ix[threshold_loc:, columns2],
                           new_db_data.iloc[threshold_loc:])
    print(np.all(is_close1))
    print(np.all(is_close2))


if __name__ == '__main__':
    import os
    file_path = join(db_path, rel_path + '.h5')
    if os.path.exists(file_path):
        os.remove(file_path)
    first_insert()
    if enable_second:
        sleep(1)
        second_insert()
        query_compare()
