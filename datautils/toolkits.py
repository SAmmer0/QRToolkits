#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/4

无法分类的工具函数
"""
def add_stock_suffix(symbol):
    '''
    向股票代码添加交易所后缀

    Parameter
    ---------
    symbol: string
        股票代码

    Return
    ------
    out: string
    '''
    if symbol.endswith('.SH') or symbol.endswith('.SZ'):
        return symbol
    SH_header = ['60', '90', '99']
    SZ_header = ['00', '30', '20']
    if symbol[:2] in SH_header:
        suffix = '.SH'
    elif symbol[:2] in SZ_header:
        suffix = '.SZ'
    else:
        raise ValueError('Invalid symbol(symbol={})'.format(symbol))
    return symbol + suffix

def drop_suffix(symbol, suffix_len=3):
    '''
    剔除代码的后缀(剔除前不做检测)

    Parameter
    ---------
    symbol: string
        代码
    suffix_len: int
        后缀长度

    Return
    ------
    out: string
    '''
    return symbol[:-suffix_len]
