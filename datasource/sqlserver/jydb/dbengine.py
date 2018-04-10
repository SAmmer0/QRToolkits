#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/10
"""
from qrtutils import parse_config
from datasource.sqlserver.utils import SQLConnector

config = parse_config('config.json')
jydb = SQLConnector(**config)
