#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/8

在测试前至少需要运行一次db_insert_test.py
"""
from pprint import pprint

from database.db import Database

db_path = r'C:\Users\c\Desktop\test\db_test'

db = Database(db_path)

pprint(db.find_collection('factor'))
pprint(db.find_data('beta'))
pprint(db.find_data('num_test'))
