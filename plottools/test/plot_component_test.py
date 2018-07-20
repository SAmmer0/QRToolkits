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
    ac = AxisComponent(np.arange(101), lambda x, pos: x % 20 == 0, lambda x, pos: str(x),
                       lambda x, pos: x % 5 == 0, label_text='测试', label_fontsize=20, grid_kwargs={'which': 'minor'},
                       enable_grid=True, ticklabel_fontsize=15, ticklabel_font=font_manager.FontProperties(fname=r'C:\Windows\Fonts\consola.ttf'))
    yac = AxisComponent(np.linspace(0, 1, 101), lambda x, pos: isclose((1000*x) % 100 , 0, abs_tol=1e-5), lambda x, pos: '{:0.2f}'.format(x),
                        enable_grid=True)
    lp(axes)
    ac(axes.xaxis)
    yac(axes.yaxis)

if __name__ == '__main__':
    main_tester(axis_test)
