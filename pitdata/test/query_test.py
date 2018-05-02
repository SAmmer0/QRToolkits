#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/5/2
"""
from pitdata import update_all
from pitdata import query, query_group

update_all()

# query single ts data
tmp1 = query('universe', '2018-01-01', '2018-03-01')
print(tmp1.head())
print(tmp1.tail())

# query single cs data
tmp2 = query('universe', '2018-01-01')
print(tmp2)
tmp3 = query('universe', '2018-01-02')
print(tmp3.head())

# query_group ts data
tmp4 = query_group(['universe', 'close'], '2018-01-01', '2018-03-01')
print(tmp4.head())

# query_group cs data
tmp5 = query_group(['universe', 'close'], '2018-01-01')
print(tmp5)
tmp6 = query_group(['universe', 'close'], '2018-01-02')
print(tmp6.head())
