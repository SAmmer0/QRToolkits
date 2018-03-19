# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:47:58 2018

@author: Hao Li
"""
from os import sep
import enum

# 相对路径开头
REL_PATH_HEADER = '.' + sep
REL_PATH_HEADER_LEN = len(REL_PATH_HEADER)

# Not a String
NaS = 'Not a String'

# file encoding
ENCODING = 'utf-8'

# 周期


class Frequency(enum.Enum):
    MONTHLY = enum.auto()
    WEEKLY = enum.auto()
    YEARLY = enum.auto()
