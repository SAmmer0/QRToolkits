#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018-05-31 15:38
""" 
from os.path import dirname, join 
from qrtutils import set_logger, parse_config

CONFIG = parse_config(join(dirname(__file__), 'config.json'))

MAIN_LOGGER_NAME = set_logger(CONFIG['log'], __file__, 'DATA_SOURCE_MAIN_LOGGER')