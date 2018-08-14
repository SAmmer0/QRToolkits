#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/8/14
"""

class Leverage(object):
    '''
    杠杆计算器基类
    提供calculate_leverage作为接口，继承类必须重载calculate_leverage以实现特殊杠杆计算器
    '''
    def calculate_leverage(self, *args, **kwargs):
        '''
        计算杠杆数据接口，默认返回杠杆为1，即无杠杆

        Parameter
        ---------
        args: tuple
            位置参数
        kwargs: dictionary
            键值参数

        Return
        ------
        leverage: float
        '''
        return 1
