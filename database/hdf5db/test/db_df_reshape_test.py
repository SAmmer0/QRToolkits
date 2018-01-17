#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-01-17 15:13:21
# @Author  : Hao Li (howardlee_h@outlook.com)
# @Link    : https://github.com/SAmmer0
# @Version : $Id$
from random import shuffle, seed
from time import sleep

import numpy as np

from fmanager import query
from database.hdf5db.dbcore import *

start_time = '2017-01-01'
first_end_time = '2017-06-01'
second_end_time = '2017-12-30'
test_factor = 'CLOSE'
db_path = r'C:\Users\c\Desktop\test\test_reshape.h5'
initial_size = 1000

seed(1)


def first_insert():
    data = query(test_factor, (start_time, first_end_time)).iloc[:, :initial_size]
    columns = list(data.columns)
    shuffle(columns)
    data = data.loc[:, columns]
    db = DBConnector.create_datafile(db_path, DataCate.PANEL)
    db.insert(data)


def second_insert():
    data = query(test_factor, (start_time, second_end_time))
    db = DBConnector.init_from_file(db_path)
    db.insert(data)


def query_compare():
    old_db_data = query(test_factor, (start_time, second_end_time))
    threshold_loc = old_db_data.index.get_loc(first_end_time) + 1
    db = DBConnector.init_from_file(db_path)
    new_db_data = db.query(start_time, second_end_time)
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
    if os.path.exists(db_path):
        os.remove(db_path)
    first_insert()
    sleep(3)
    second_insert()
    query_compare()
