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
        传入到主绘图组件的除数据外的其他参数
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
            print(cls_name)
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

