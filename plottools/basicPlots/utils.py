#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/17
"""
import numpy as np

class PlotComponentBase(object):
    '''
    绘图组件基类
    '''
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, obj):
        '''
        将对象转换为可以调用的状态，该函数主要负责具体的绘图逻辑

        Parameter
        ---------
        obj: object
            当前情境下，可能为matplotlib.axes.Axes或者matplotlib.axis.Axis，即用于
            处理的子图或者坐标轴
        '''
        pass

class LinePlot(PlotComponentBase):
    '''
    单线条绘制类

    Parameter
    ---------
    x: numpy.array or the like
        需要绘制的一维数据，必须有shape属性，如果y参数为None，该参数表示y轴数据，对应的x
        轴数据为range(0, len(x))，如果y参数部位None，则表示x轴数据
    y: numpy.array or the like, default None
        y轴数据，必须有shape属性
    secondary_y: bool, default False
        用于标识数据的y轴是否对应于右轴(默认对应于左轴)，只能通过k=w的形式传入
    kwargs: dictionary
        用于传入matplot.axes.Axes.plot的其他参数
    '''
    def __init__(self, x, y=None, *, secondary_y=False, **kwargs):
        if y is None:
            self._x = np.arange(len(x))
            self._y = x
        else:
            self._x = x
            self._y = y
        self._secondary_y = secondary_y
        self._kwargs = kwargs

    def __call__(self, axes):
        '''
        主绘图函数

        Parameter
        ---------
        axes: matplotlib.axes.Axes
            需要绘制的子图
        '''
        if self._secondary_y:
            axes = axes.twinx()
        axes.plot(self._x, self._y, **self._kwargs)

