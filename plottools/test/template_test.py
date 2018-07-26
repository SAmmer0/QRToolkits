#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/25
"""

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from pitdata import query_group, query
from tdtools import get_calendar
from plottools.basicPlots.template import DateXAxisPlot, function_creator_generator, PlotTemplateBase, object_creator_generator
from plottools.basicPlots.components import KLinePlot, StackPlot, LinePlot

def main_tester(test_func):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    test_func(ax)

def datexaxis_test(ax):
    data = query_group(['OPEN', 'CLOSE', 'HIGH', 'LOW'], '2018-01-01', '2018-07-25')
    data = data.iloc[:, 0].unstack()
    def creator(*data, **kwargs):
        return KLinePlot(*data, **kwargs)
    calendar = get_calendar('stock.sse')
    plotter = DateXAxisPlot(creator, lambda x: calendar.is_cycle_target(x, 'MONTHLY', 'LAST'),
                            xaxis_kwargs={'ticklabel_kwargs': {'rotation': 30}})
    plotter.plot(ax, data['OPEN'], data['CLOSE'], data['HIGH'], data['LOW'], axis_dates=data.index)

def datexaxis_stackplot_test(ax):
    data = query('CS500_CLOSE', '2010-01-01', '2018-07-25')
    oneyr_mdd = data.rolling(250, 250).apply(lambda x: x[-1] / np.max(x) - 1)
    def creator(*data, **kwargs):
        return StackPlot(np.arange(len(data[0])), *data)
    plotter = DateXAxisPlot(creator, lambda x: get_calendar('stock.sse').is_cycle_target(x, 'YEARLY', 'LAST'),
                            xaxis_kwargs={'ticklabel_kwargs': {'rotation': 30}})
    plotter.plot(ax, oneyr_mdd, axis_dates=oneyr_mdd.index)

def func_creator_generator_test(ax):
    data = np.random.randn(1000)
    creator = function_creator_generator(sns.kdeplot, 'ax')
    plotter = PlotTemplateBase(creator)
    plotter.plot(ax, data)

def obj_creator_generator_test(ax):
    data = np.random.randn(1000)
    creator = object_creator_generator(LinePlot)
    plotter = PlotTemplateBase(creator)
    plotter.plot(ax, data)

if __name__ == '__main__':
    # main_tester(datexaxis_stackplot_test)
    # main_tester(datexaxis_test)
    # main_tester(func_creator_generator_test)
    main_tester(obj_creator_generator_test)
