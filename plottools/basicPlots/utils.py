#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/17
"""
import numpy as np

from plottools.basicPlots.mpl_finance import candlestick2_ohlc
from plottools.basicPlots.const import HT_FONT

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

# --------------------------------------------------------------------------------------------------
# 绘图部分
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
        用于标识数据的y轴是否对应于右轴(默认对应于左轴)
    kwargs: dictionary
        用于传入matplot.axes.Axes.plot的其他参数
    '''
    def __init__(self, x, y=None, secondary_y=False, **kwargs):
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

class BarPlot(PlotComponentBase):
    '''
    条形图

    Parameter
    ---------
    x: np.array
        标签的位置
    data: np.array or the like
        每个标签对应的数据的高度
    vertical: boolean, default True
        是否为纵向条形图，如果为False，将绘制横向条形图
    secondary_y: boolean, default False
        是否绘制在右轴
    kwargs: dictionary
        其他需要传入到bar或者barh中的参数
    '''
    def __init__(self, x, data, vertical=True, secondary_y=False, **kwargs):
        self._x = x
        self._data = data
        if vertical:
            self._func_name = 'bar'
        else:
            self._func_name = 'barh'
        self._secondary_y = secondary_y
        self._kwargs = kwargs

    def __call__(self, axes):
        if self._secondary_y:
            axes = axes.twinx()
        plot_func = getattr(axes, self._func_name)
        plot_func(self._x, self._data, **self._kwargs)

class KLinePlot(PlotComponentBase):
    '''
    K线图

    Parameter
    ---------
    open_price: np.array or the like
        开盘价
    close_price: np.array or the like
        收盘价
    high_price: np.array or the like
        最高价
    low_price: np.array or the like
        最低价
    color_up: string, default red
        上涨颜色
    color_down: string, default green
        下跌颜色
    alpha: float, default 0.75
        k线透明度
    width: float, default 0.5
        k线宽度
    secondary_y: boolean, False
        是否适用右轴
    '''
    def __init__(self, open_price, close_price, high_price, low_price,
                 color_up='red', color_down='green', alpha=.75, width=0.5,
                 secondary_y=False):
        self._data = (open_price, high_price, low_price, close_price)
        self._color_up = color_up
        self._color_down = color_down
        self._secondary_y = secondary_y
        self._alpha = alpha
        self._width = width

    def __call__(self, axes):
        if self._secondary_y:
            axes = axes.twinx()
        candlestick2_ohlc(axes, *self._data, colorup=self._color_up,
                          colordown=self._color_down, alpha=self._alpha,
                          width=self._width)

class HistPlot(PlotComponentBase):
    '''
    直方图

    Parameter
    ---------
    data: np.array or the like
        绘制数据
    bins: int
        区间数
    secondary_y: boolean, default False
        是否适用右轴
    **kwargs: dictionary
        其他传入到hist中的参数
    '''
    def __init__(self, data, bins, secondary_y=False, **kwargs):
        self._data = data
        self._bins = bins
        self._secondary_y = secondary_y
        self._kwargs = kwargs

    def __call__(self, axes):
        if self._secondary_y:
            axes = axes.twinx()
        axes.hist(self._data, self._bins, **self._kwargs)

class HeatMap(PlotComponentBase):
    '''
    热力图

    Parameter
    ---------
    data: numpy.ndarray
        二维矩阵数据
    add_colorbar: boolean, default True
        标识是否添加colorbar
    colorbar_label: string, default ""
        colorbar的标签
    cb_kwargs: dictionary, default None
        colorbar的一些相关参数
    kwargs: dictionary
        其他传入到imshow的参数
    '''
    def __init__(self, data, add_colorbar=True, colorbar_label="", cb_kwargs=None, **kwargs):
        self._data = data
        self._add_colorbar_flag = add_colorbar
        self._colorbar_label = colorbar_label
        if cb_kwargs is None:
            self._cb_kwargs = {}
        else:
            self._cb_kwargs = cb_kwargs
        self._kwargs = kwargs

    def __call__(self, axes):
        im = axes.imshow(self._data, **self._kwargs)
        if self._add_colorbar_flag:
            self._add_colorbar(axes, im, self._colorbar_label, **self._cb_kwargs)

    def _add_colorbar(self, axes, im, label, **kwargs):
        '''
        向热力图中添加colorbar

        Parameter
        ---------
        axes: matplotlib.axes.Axes
            绘制热力图的子图
        im: matplotlib.image.AxesImage
            热力图对象
        label: string
            colorbar标签
        kwargs: dictionary
            其他传入matplotlib.figure.Figure.colorbar中的参数
        '''
        cbar = axes.figure.colorbar(im, ax=axes, **kwargs)
        cbar.ax.set_ylabel(label, rotation=-90, va='bottom')

class StackPlot(PlotComponentBase):
    '''
    层叠图

    Parameter
    ---------
    x: np.array or the like
        x轴坐标，长度为N
    y: np.array or the like
        y轴坐标，为M*N或者一系列N维向量
    labels: iterable, default None
        元素为字符串，labels的长度为N
    colors: iterable, default None
        元素为matplotlib中可以接受的颜色，colors长度为N，仅通过关键字设置
    secondary_y: boolean, default False
        是否适用右轴
    kwargs: dictionary
        其他传入到matplotlib.axes.Axes.stackplot中的参数，仅通过关键字设置
    '''
    def __init__(self, x, *y, labels=None, colors=None, secondary_y=False, **kwargs):
        self._x = x
        self._y = y
        self._kwargs = kwargs
        if labels is not None:
            self._kwargs.update(labels=labels)
        if colors is not None:
            self._kwargs.update(colors=colors)
        self._secondary_y = secondary_y

    def __call__(self, axes):
        if self._secondary_y:
            axes = axes.twinx()
        axes.stackplot(self._x, *self._y, **self._kwargs)

class ScatterPlot(PlotComponentBase):
    '''
    散点图

    Parameter
    ---------
    x: np.array or the like
        x轴坐标
    y: np.array or the like, default None
        y轴坐标，None表示x参数作为y轴坐标，x轴坐标为np.arange(0, len(x))
    secondary_y: boolean, default False
        是否适用右轴
    kwargs: dictionary
        其他传入到matplotlib.axes.Axes.scatter中的参数
    '''
    def __init__(self, x, y=None, secondary_y=False, **kwargs):
        if y is None:
            self._y = x
            self._x = np.arange(len(x))
        else:
            self._x = x
            self._y = y
        self._secondary_y = secondary_y
        self._kwargs = kwargs

    def __call__(self, axes):
        if self._secondary_y:
            axes = axes.twinx()
        axes.scatter(self._x, self._y, **self._kwargs)


class CompositePlots(PlotComponentBase):
    '''
    合成图类，用于结合其他绘图组件，生成综合性图片

    Parameter
    ---------
    plot_components: iterable
        元素为PlotComponentsBase的子类的实例
    '''
    def __init__(self, *plot_components):
        self._plot_components = plot_components

    def __call__(self, axes):
        for plot_kit in self._plot_components:
            plot_kit(axes)

# --------------------------------------------------------------------------------------------------
# title部分
class TitleComponent(PlotComponentBase):
    '''
    绘图过程中的标题组件

    Parameter
    ---------
    text: string
        标题内容
    font_properties: matplotlib.font_manager.FontProperties, default HT_FONT
        主要用于设置字体
    font_size: int or string, default None
        用于设置字体大小
        之所以把fontproperties和fontsize单独提取出来是因为
        两个参数在axes.set_title中的顺序会影响到最终的效果，只有
        当fontproperties在fontsize前时，修改字体大小的操作才会
        有效
    kwargs: dictionary
        除上述两个关键字外，其他需要传入到axes.set_title中的参数
    '''
    def __init__(self, text, font_properties=HT_FONT, font_size=None, **kwargs):
        self._text = text
        # 这种解决方式可能仍然存在问题，但是目前能够使用
        self._font_kwargs = {}
        if font_properties is not None:
            self._font_kwargs.update(fontproperties=font_properties)
        if font_size is not None:
            self._font_kwargs.update(fontsize=font_size)
        self._kwargs = kwargs

    def __call__(self, axes):
        axes.set_title(self._text, **self._font_kwargs, **self._kwargs)

