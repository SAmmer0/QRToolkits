#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/16
"""
import pdb
import re

from plottools.basicPlots.generalplots import PlotFramework
from plottools.basicPlots.components import AxisComponent, date_ticker

# --------------------------------------------------------------------------------------------------
# 模板
class PlotTemplateBase(object):
    '''
    该类提供setc_?开头的方法用来处理组件个性化设置的相关操作

    Parameter
    ---------
    main_plot_creator: function(*data, *kwargs)->func(axes)
        用于创建主绘图功能组件
    main_plot_kwargs: dictionary
        传入到creator中的除数据外的其他参数
    '''
    def __init__(self, main_plot_creator, **main_plot_kwargs):
        self._main_plot_creator = main_plot_creator
        self._main_plot_kwargs = main_plot_kwargs
        self._handlers = {}

    def __getattr__(self, item):
        '''
        重载，用于处理组件个性化接口问题

        Parameter
        ---------
        item: string
            方法或者属性名，当前支持的方法格式为setc_?，其中?的可选如下
            ['title', 'legend', 'annotate', 'xaxis', 'yaxis', 'secondary_yaxis', 'others']

        Return
        ------
        res: Object
            对应的方法或者属性
        '''
        if item.startswith('setc_'):
            cls_name = re.match('setc_([_0-9a-zA-Z]*)', item).groups()[0]
            valid_settings = ['title', 'legend', 'annotate', 'xaxis', 'yaxis', 'secondary_yaxis', 'others']
            if cls_name not in valid_settings:
                raise AttributeError
            def set_handler(handler_obj):
                self._handlers[cls_name] = handler_obj
            return set_handler
        else:
            raise AttributeError

    def plot(self, axes, *data):
        '''
        执行绘图

        Parameter
        ---------
        axes: WrappedTwinAxes
            子图
        data: tuple
            绘图相关的数据

        Return
        ------
        axes: WrappedTwinAxes
            完成绘图后的子图
        '''
        self._handlers.update(main_plot=self._main_plot_creator(*data, **self._main_plot_kwargs))
        plot_framework = PlotFramework(axes, self._handlers)
        res_axes = plot_framework.plot()
        return res_axes

class DateXAxisPlot(PlotTemplateBase):
    '''
    x轴为时间时的绘图处理函数

    Parameter
    ---------
    main_plot_creator: function(*data, **kwargs)->func(axes)
        主绘图函数
    date_locator: function(date)->boolean
        计算当前tick是否为刻度的定位函数
    date_formatter: string, default '%Y-%m-%d'
        使用date_fromatter.format(date)来获取对应刻度的标签
    main_plot_kwargs: dictionary, default None
        传入到主绘图组件中的其他参数
    xaxis_kwargs: dictionary, default None
        传入到AxisComponent中的关键字参数
    '''
    def __init__(self, main_plot_creator, date_locator, date_formatter='{:%Y-%m-%d}',
                 add_header=True, add_tail=False, main_plot_kwargs=None, xaxis_kwargs=None):
        if main_plot_kwargs is None:
            self._main_plot_kwargs = {}
        else:
            self._main_plot_kwargs = main_plot_kwargs
        if xaxis_kwargs is None:
            self._xaxis_kwargs = {}
        else:
            self._xaxis_kwargs = xaxis_kwargs
        super().__init__(main_plot_creator, **self._main_plot_kwargs)
        self._ticker_kwargs = {'locator': date_locator, 'formatter': date_formatter,
                               'add_header': add_header, 'add_tail': add_tail}

    def plot(self, axes, *data, axis_dates):
        '''
        执行绘图

        Parameter
        ---------
        axes: WrappedTwinAxes
            子图
        data: tuple
            绘图的相关数据
        axis_dates: iterable, keyword only
            日期轴数据

        Return
        ------
        axes: WrappedTwinAxes
            完成绘图后的子图
        '''
        major_ticks_pos, major_ticks_labels = date_ticker(axis_dates, **self._ticker_kwargs)
        ac = AxisComponent(major_ticks_pos, major_ticks_labels, **self._xaxis_kwargs)
        self.setc_xaxis(ac)
        super().plot(axes, *data)

# --------------------------------------------------------------------------------------------------
# creator helper
def object_creator_generator(cls):
    def creator(*data, **kwargs):
        obj = cls(*data, **kwargs)
        return obj
    return creator

def function_creator_generator(func, ax_parameter):
    '''
    用于生成返回函数的creator

    Parameter
    ---------
    func: function(*args, **kwargs)
        核心绘图函数
    ax_parameter: string
        绘图轴对应的参数名称

    Return
    ------
    creator: function(*data, **kwargs)
    '''
    def creator(*data, **kwargs):
        def plot_func(ax):
            kwargs[ax_parameter] = ax
            func(*data, **kwargs)
        return plot_func
    return creator
