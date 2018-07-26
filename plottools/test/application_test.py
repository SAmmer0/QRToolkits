#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/26
"""
import pdb
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from tdtools import get_calendar
from plottools.basicPlots.applications import plot_nav
from plottools.layout.template import NLineLayout

def main_tester(func):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    func(ax)

def plot_nav_test(ax):
    tds = get_calendar('stock.sse').get_tradingdays('2016-01-01', '2018-07-25')
    data = pd.DataFrame(np.random.rand(len(tds), 3), index=tds, columns=['测试1', 'test2', 'test3'])
    data = (data / 5 - 0.1).add(1).cumprod()
    # pdb.set_trace()
    plot_nav(ax, data, title='净值函数测试', xlabel='x轴标签', ylabel='y轴标签', legend=True, linewidth=1.5)

def nav_layout_test():
    nll = NLineLayout(2)
    while True:
        ax = nll.get_next_axes()
        if ax is None:
            break
        plot_nav_test(ax)

if __name__ == '__main__':
    # main_tester(plot_nav_test)
    nav_layout_test()
