#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/16
"""

class PlotBase(object):
    '''
    子图绘制基类
    通过以下提供的接口进行设置

    Parameter
    ---------
    axes: matplotlib.axes.Axes
        用于绘制的子图
    callbacks: dictionary
        用于设置回调函数的字典，格式如下{callback_field: {'function': callback, 'kwargs': kwargs}
        (或者任何支持get(k, d=None))的对象}，该对象的域至少包含['function', 'kwargs']，function格式为
        function(axes or axis, **kwargs)，kwargs可以为None表示无参数
        callback_field如下(不要求对下述field均做设置，不包含的field视为不需要设置)：
        main_plot:主图绘制，格式为function(axes, **kwargs)
        title:设置子图title，格式为function(axes, **kwargs)
        legend:设置子图legend，格式为function(axes, **kwargs)
        annotate:设置子图标记，格式为function(axes, **kwargs)
        xaxis:设置x轴，格式为function(axis, **kwargs)
        yaxis:设置y轴，格式为function(axis, **kwargs)
        others:设置其他属性，格式为function(axis, **kwargs)
    '''
    def __init__(self, axes, callbacks):
        self._axes = axes
        self._callbacks = callbacks
        self._call_order = ['main_plot', 'xaxis', 'yaxis', 'title', 'legend', 'annotate', 'others']    # 功能调用顺序

    def pset_main_plot(self, callback, **kwargs):
        '''
        绘制主图
        '''
        callback(self._axes, **kwargs)

    def pset_title(self, callback, **kwargs):
        '''
        设置title
        '''
        callback(self._axes, **kwargs)

    def pset_legend(self, callback, **kwargs):
        '''
        设置legend
        '''
        callback(self._axes, **kwargs)

    def pset_annotate(self, callback, **kwargs):
        '''
        设置annotate
        '''
        callback(self._axes, **kwargs)

    def pset_xaxis(self, callback, **kwargs):
        '''
        设置x轴
        '''
        callback(self._axes.get_xaxis(), **kwargs)

    def pset_yaxis(self, callback, **kwargs):
        '''
        设置y轴
        '''
        callback(self._axes.get_yaxis(), **kwargs)

    def pset_others(self, callback, **kwargs):
        '''
        设置其他属性
        '''
        callback(self._axes, **kwargs)

    def plot(self):
        '''
        绘制图片
        '''
        for field in self._call_order:
            artist_handler = self._callbacks.get(field)
            if artist_handler is None:    # 没有添加当前field下的设置
                continue
            callback = artist_handler.get('function')
            kwargs = artist_handler.get('kwargs')
            if kwargs is None:
                kwargs = {}

            handler = getattr(self, 'pset_' + field)
            handler(callback, **kwargs)
