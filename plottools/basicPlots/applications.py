#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/26
"""
import pdb
import pandas as pd
import numpy as np

from plottools.basicPlots.template import PlotTemplateBase, DateXAxisPlot
from plottools.basicPlots.components import (LinePlot, BarPlot, KLinePlot,
                                             AxisComponent, CompositePlots, num_multiple_ticker,
                                             TitleComponent, AxisLimitSetter, LegendComponent)
from tdtools import get_calendar

def plot_nav(axes, data, normalize=True, title=None, xlabel=None, ylabel=None,
             legend=False, colors=None, linewidth=None):
    '''
    绘制净值图

    Parameter
    ---------
    axes: plottools.generalplots.WrappedTwinAxes
        绘制的子图
    data: pandas.DataFrame or pandas.Series
        数据，要求index为日期
    normalize: boolean, default True
        是否对数据进行正则化(正则化至将输入的数据转换为标准的净值数据，即所有数据除以最早日期的数据)
    title: string, default None
        标题
    xlabel: string, default None
        x轴标签
    ylabel: string, default None
        y轴标签
    legend: boolean, default False
        是否添加legend
    colors: iterable, default None
        线条颜色
    linewidth: float, default None
        线条宽度

    Return
    ------
    axes: WrappedTwinAxes
        绘图后的子图
    '''
    if normalize:
        data = data / data.iloc[0]
    # 计算数据的范围
    data_max = data.max()
    data_min = data.min()
    if isinstance(data_max, pd.Series):
        data_max = data_max.max()
        data_min = data_min.min()

    date_idx = data.index
    if isinstance(data, pd.Series):
        data = [data]
    elif isinstance(data, pd.DataFrame):
        data = [data[c] for c in data.columns]
    # 设置绘图相关参数
    plot_kwargs = [{} for i in range(len(data))]
    if colors is not None:
        if len(colors) != len(data):
            raise ValueError('colors parameter should have the same length as data column!')
        for d, c in zip(plot_kwargs, colors):
            d.update(color=c)
    if linewidth is not None:
        for d in plot_kwargs:
            d.update(linewidth=linewidth)
    # 设置legend
    if legend:
        for d in data:
            if d.name is None:
                legend = False
                break
    if legend:
        for d, n in zip(plot_kwargs, data):
            d.update(label=n.name)
    # creator
    def creator(*data, **kwargs):
        def plotter(ax):
            args = kwargs['args']
            lps = [LinePlot(d, **k) for d, k in zip(data, args)]
            cp = CompositePlots(*lps)
            cp(ax)
        return plotter

    # 确定x轴主刻度轴频率
    calendar = get_calendar('stock.sse')
    gap_days = (date_idx[-1] - date_idx[0]).days
    if gap_days / 250 > 3:  # 时间过长，以年为周期
        freq = 'YEARLY'
    elif gap_days / 7 > 20:  # 中等时间，以月为周期
        freq = 'MONTHLY'
    else:   # 短时间，以周为周期
        freq = 'WEEKLY'
    locator = lambda x: calendar.is_cycle_target(x, freq, 'LAST')

    # 设置主绘图框架
    plotter = DateXAxisPlot(creator, locator, xaxis_kwargs={'label_text': xlabel,
                                                            'ticklabel_kwargs': {'rotation': 30}},
                            main_plot_kwargs={'args': plot_kwargs})
    # 设置y轴
    y_range = data_max - data_min
    if y_range > 20:
        y_major_base = y_range // 10
        y_minor_base = y_major_base / 2
    if y_range > 10:
        y_major_base = 2
        y_minor_base = 1
    elif y_range > 5:
        y_major_base = 1
        y_minor_base = 0.5
    else:
        y_major_base = 0.5
        y_minor_base = 0.25
    y_major_pos, y_major_label = num_multiple_ticker(data_min, data_max, y_major_base, '{:0.1f}')
    y_minor_pos, _ = num_multiple_ticker(data_min, data_max, y_minor_base, '{:0.1f}')
    yaxis = AxisComponent(y_major_pos, y_major_label, y_minor_pos, label_text=ylabel)
    plotter.setc_yaxis(yaxis)
    # 设置title
    if title is not None:
        title_component = TitleComponent(title, font_size=15)
        plotter.setc_title(title_component)
    # 设置轴的范围
    xaxis_limit_setter = AxisLimitSetter(0, len(date_idx), 'x')
    yaxis_limit_setter = AxisLimitSetter(data_min - y_minor_base, data_max + y_minor_base, 'y')
    axis_limit_setter = CompositePlots(xaxis_limit_setter, yaxis_limit_setter)
    plotter.setc_others(axis_limit_setter)

    # 设置legend
    if legend:
        legend_component = LegendComponent()
    plotter.setc_legend(legend_component)

    plotter.plot(axes, *data, axis_dates=date_idx)
