#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/16
"""

class WrappedTwinAxes(object):
    '''
    未解决axes.twinx或者axes.twiny在每次调用时都生成一个不同的对象的问题，
    对axes进行包装，以保证twin?返回对象的唯一性

    Parameter
    ---------
    axes: matplotlib.axes.Axes
        子图
    '''
    def __init__(self, axes):
        self._axes = axes
        self._second_axes = {}

    def twinx(self):
        if 'x' not in self._second_axes:
            second_axes = self._axes.twinx()
            self._second_axes['x'] = second_axes
            return second_axes
        else:
            return self._second_axes['x']
    def twiny(self):
        if 'y' not in self._second_axes:
            second_axes = self._axes.twiny()
            self._second_axes['y'] = second_axes
            return second_axes
        else:
            return self._second_axes['y']

    def __getattr__(self, item):
        return getattr(self._axes, item)


class PlotBase(object):
    '''
    子图绘制基类
    通过以下提供的接口进行设置

    Parameter
    ---------
    axes: matplotlib.axes.Axes
        用于绘制的子图
    callbacks: dictionary
        用于设置回调函数的字典，格式如下{callback_field: function}
        (或者任何支持get(k, d=None))的对象，function格式为function(axes or axis)
        callback_field如下(不要求对下述field均做设置，不包含的field视为不需要设置)：
        main_plot:主图绘制，格式为function(axes)
        title:设置子图title，格式为function(axes)
        legend:设置子图legend，格式为function(axes)
        annotate:设置子图标记，格式为function(axes)
        xaxis:设置x轴，格式为function(axis)
        secondary_xaxis:设置x轴右轴，格式为function(axis)
        yaxis:设置y轴，格式为function(axis)
        others:设置其他属性，格式为function(axis)
    '''
    def __init__(self, axes, callbacks):
        self._axes = WrappedTwinAxes(axes)
        self._second_axis = None
        self._callbacks = callbacks
        self._call_order = ['main_plot', 'xaxis', 'secondary_xaxis',
                            'yaxis', 'title', 'legend', 'annotate', 'others']    # 功能调用顺序

    def pset_main_plot(self, callback):
        '''
        绘制主图
        '''
        callback(self._axes)

    def pset_title(self, callback):
        '''
        设置title
        '''
        callback(self._axes)

    def pset_legend(self, callback):
        '''
        设置legend
        '''
        callback(self._axes)

    def pset_annotate(self, callback):
        '''
        设置annotate
        '''
        callback(self._axes)

    def pset_xaxis(self, callback):
        '''
        设置x轴
        '''
        callback(self._axes.get_xaxis())

    def pset_secondary_xaxis(self, callback):
        '''
        设置右边x轴
        '''
        callback(self._axes.twinx().get_xaxis())

    def pset_yaxis(self, callback):
        '''
        设置y轴
        '''
        callback(self._axes.get_yaxis())

    def pset_others(self, callback):
        '''
        设置其他属性
        '''
        callback(self._axes)

    def plot(self):
        '''
        绘制图片
        '''
        for field in self._call_order:
            callback = self._callbacks.get(field)
            if callback is None:    # 没有添加当前field下的设置
                continue
            handler = getattr(self, 'pset_' + field)
            handler(callback)

