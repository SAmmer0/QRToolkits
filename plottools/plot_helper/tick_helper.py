#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/27
"""

def date_ticker(dates, locator, formatter='{:%Y-%m-%d}', add_header=True, add_tail=False):
    '''
    生成给定日期序列的刻度位置及标签

    Parameter
    ---------
    dates: iterable
        元素为datetime及其子类
    locator: function(date)->boolean
        返回True表示以该点为刻度
    formatter: string, default '{:%Y-%m-%d}'
        使用formatter.format(date)获取对应刻度的标签
    add_header: boolean, default True
        是否加入第一个日期
    add_tail: boolean, default False
        是否加入最后一个日期

    Return
    ------
    pos: list
        刻度位置，元素为int
    labels: list
        刻度标签，元素为string
    '''
    pos = [i for i, d in enumerate(dates) if locator(d)]
    if add_header and 0 not in pos:
        pos = [0] + pos
    last_pos = len(dates) - 1
    if add_tail and last_pos not in pos:
        pos.append(last_pos)
    labels = [formatter.format(dates[i]) for i in pos]
    return pos, labels

