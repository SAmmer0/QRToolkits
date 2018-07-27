#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/27
"""

from matplotlib import rc_context

def chinese_font_context():
    '''
    返回一个中文字体绘图环境

    Return
    ------
    context_manager
    '''
    return rc_context(rc={'font.sans-serif': ['Microsoft YaHei']})
