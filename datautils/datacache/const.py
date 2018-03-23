#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/22
"""
import enum

class CacheStatus(enum.Enum):
    FUTURE = enum.auto()
    PAST = enum.auto()
    BOTH = enum.auto()
    ENOUGH = enum.auto()
