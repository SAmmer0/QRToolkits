#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/25
"""

import matplotlib.pyplot as plt

from pitdata import query_group
from tdtools import get_calendar
from plottools.basicPlots.template import DateXAxisPlot
from plottools.basicPlots.components import KLinePlot

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

if __name__ == '__main__':
    main_tester(datexaxis_test)
