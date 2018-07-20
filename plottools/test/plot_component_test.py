#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/20
"""
from math import isclose
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import font_manager
from plottools.basicPlots.utils import (AxisComponent,
                                        LinePlot)

def main_tester(test_func):
    # test_func格式为：test_func(axes)
    fig = plt.figure()
    axes = fig.add_subplot(111)
    test_func(axes)

def axis_test(axes):
    lp = LinePlot(np.random.rand(100))
    xtick = range(0, 101, 10)
    ac = AxisComponent(xtick, [str(i) for i in xtick], label_text='测试', label_fontsize=20, grid_kwargs={'which': 'minor'},
                       enable_grid=True, ticklabel_fontsize=15, ticklabel_font=font_manager.FontProperties(fname=r'C:\Windows\Fonts\consola.ttf'))
    ytick = np.arange(0, 1.1, 0.1)
    yac = AxisComponent(ytick, ['{:0.2f}'.format(i) for i in ytick], enable_grid=True)
    lp(axes)
    ac(axes.xaxis)
    yac(axes.yaxis)

if __name__ == '__main__':
    main_tester(axis_test)
