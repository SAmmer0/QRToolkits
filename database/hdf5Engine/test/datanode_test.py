#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/1
"""
import json

from database.db import DataNode

sample_path = r'C:\Users\c\Desktop\test\db_meta_test.json'

with open(sample_path, 'r', encoding='utf-8') as f:
    meta_data = json.load(f)
    root = DataNode.init_from_meta(meta_data)
print('Done!')
