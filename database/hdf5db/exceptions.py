# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:49:16 2018

@author: Hao Li

定义一些基本的异常类型
"""
# 不支持的数据类型
class UnsupportedDataTypeError(Exception):
    pass

# 无效的输入参数类型
class InvalidInputTypeError(Exception):
    pass

