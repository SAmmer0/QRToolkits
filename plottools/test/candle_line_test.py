#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/27
"""

import matplotlib.pyplot as plt
from plottools.plot_helper.kline_helper import plot_candle_line
import pitdata
from tdtools import get_calendar

def test():
    fig = plt.figure()
    ax = fig.add_subplot(111)
    data = pitdata.query_group(['OPEN', 'CLOSE', 'HIGH', 'LOW'], '2017-01-01', '2018-07-27')
    data = data.iloc[:, 0].unstack()
    plot_candle_line(ax, data, columns=('OPEN', 'CLOSE', 'HIGH', 'LOW'),
                     majortick_locator=lambda x: get_calendar('stock.sse').is_cycle_target(x, 'YEARLY', 'FIRST'),
                     majortick_format='{:%Y-%m}',
                     minortick_locator=lambda x: get_calendar('stock.sse').is_cycle_target(x, 'MONTHLY', 'FIRST'),
                     minortick_format='{:%m}')

if __name__ == '__main__':
    test()
