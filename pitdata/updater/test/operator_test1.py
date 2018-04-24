#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/24
"""
from tdtools import trans_date
from pitdata.updater.operator import load_metadata, update_single_data, dump_metadata
from pitdata.updater.operator import METADATA_FILENAME
from pitdata.updater.loader import load_all

data_dict = load_all()
universe_dd = data_dict['universe']
meta_data = load_metadata(METADATA_FILENAME)

update_single_data(universe_dd, trans_date('2018-01-01'), trans_date('2018-04-01'), meta_data)
dump_metadata(meta_data, METADATA_FILENAME)
