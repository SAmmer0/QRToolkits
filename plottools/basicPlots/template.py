#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/16
"""
import re

from plottools.basicPlots.generalplots import PlotFramework, WrappedTwinAxes
from plottools.basicPlots.components import (LinePlot, BarPlot, ScatterPlot, KLinePlot,
                                             HistPlot, HeatMap, StackPlot, secondary_axis_wrapper,
                                             TitleComponent, LegendComponent, AnnotateComponent,
                                             AxisComponent, AxisLimitSetter, num_multiple_ticker,
                                             date_ticker, CompositePlots)

# --------------------------------------------------------------------------------------------------
# handler creators
def title_creator(*data, **kwargs):
    return TitleComponent(**kwargs)

def legend_creator(*data, **kwargs):
    return LegendComponent(**kwargs)

def annotate_creator(*data, **kwargs):
    return AnnotateComponent(**kwargs)

def null_creator(*data, **kwargs):
    return None


# --------------------------------------------------------------------------------------------------
# 模板
class PlotTemplateBase(object):
    '''
    绘图模板基类，该类主要用于对所有常用绘图组件进行整合，简化整个绘图流程
    该类提供初始化的方式来简化相关组件的设置，同时也提供setc_?开头的方法用来
    处理组件个性化的相关操作

    Parameter
    ---------
    data: tuple
        绘图相关的数据

    以下参数仅支持键值的方式传入
    main_plot_kwargs: dictionary, default None
        主绘图组件的其他参数
    title_kwargs: dictionary, default None
        标题组件的其他参数
    legend_kwargs: dictionary, default None
        legend组件的其他参数
    annotate_kwargs: dictionary, default None
        annotate组件的其他参数
    xaxis_kwargs: dictionary, default None
        x轴组件的其他参数
    yaxis_kwargs: dictionary, default None
        y轴组件的其他参数
    secondary_yaxis_kwargs: dictionary, default None
        y轴右轴组件的其他参数
    others_kwargs: dictionary, default None
        其他额外组件的参数
    '''
    def __init__(self, *data, main_plot_kwargs=None, title_kwargs=None, legend_kwargs=None,
                 annotate_kwargs=None, xaxis_kwargs=None, yaxis_kwargs=None, secondary_yaxis_kwargs=None,
                 others_kwargs=None):
        self._data = data
        self._kwargs = self._filter_kwargs(main_plot_kwargs=main_plot_kwargs, title_kwargs=title_kwargs,
                                           legend_kwargs=legend_kwargs, annotate_kwargs=annotate_kwargs,
                                           xaxis_kwargs=xaxis_kwargs, yaxis_kwargs=yaxis_kwargs,
                                           secondary_yaxis_kwargs=secondary_yaxis_kwargs, others_kwargs=others_kwargs)
        # TODO：在该字典中添加用于创建组件handler的函数(或者callable方法)，字典的格式为{组件名: function}
        # function的格式为function(*self._data, **kwargs)->instance，其中kwargs指对应组件的kwargs
        # 可以设置的组件名仅包含[plot, title, legend, annotate, xaxis, yaxis, secondary_yaxis, other]
        self._handlers_creator = {}
        self._handlers = {}
        self._set_handlers()

    def _set_handlers(self):
        '''
        初始化所有handler
        '''
        self._handlers = {k: self._handlers_creator[k](*self._data, **self._kwargs[k])
                          for k in self._handlers_creator}

    def _filter_kwargs(self, **kwargs):
        '''
        用于过滤一些参数，仅保留非None的参数

        Parameter
        ---------
        kwargs: dictionary
            需要过滤的参数

        Return
        ------
        out: dictionary
            仅保留值为非None的参数
        '''
        out = {k: v if v is not None else {} for k, v in kwargs.items()}
        return out

    def _update_hander(self, handler_name):
        '''
        对handler进行更新，使用当前数据和相关参数

        Parameter
        ---------
        handler_name: string
            handler的名称
        '''
        handler_creator = self._handlers_creator[handler_name]
        kwargs = self._kwargs[handler_name]
        handler = handler_creator(*self._data, **kwargs)
        self._handlers[handler_name] = handler

    def __getattr__(self, item):
        '''
        重载，用于处理组件个性化接口问题

        Parameter
        ---------
        item: string
            方法或者属性名

        Return
        ------
        res: Object
            对应的方法或者属性
        '''
        if item.startswith('setc_'):
            cls_name = re.match('setc_([0-9a-zA-Z])', item).groups()[0]
            def set_handler(*args, **kwargs):
                # 如果有顺序参数，则直接将第一个顺序参数更新为对应的handler
                # 如果没有，则使用kwargs数据对handler相关的kwargs数据进行更新，并使用
                # 更新后的数据创建handler
                if len(args) > 0:
                    self._handlers[cls_name] = args[0]
                else:
                    handler_kwargs = self._kwargs[cls_name]
                    handler_kwargs.update(kwargs)
                    self._update_handler(cls_name)
            return set_handler
        else:
            raise AttributeError

    def plot(self, axes):
        '''
        执行绘图

        Parameter
        ---------
        axes: WrappedTwinAxes
            子图

        Return
        ------
        axes: WrappedTwinAxes
            完成绘图后的子图
        '''
        plot_framework = PlotFramework(axes, self._handlers)
        res_axes = plot_framework.plot()
        return res_axes

