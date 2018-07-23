#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/17
"""

import pdb
from functools import wraps
import numpy as np
from matplotlib.pyplot import setp

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
    kwargs: dictionary
        用于传入matplot.axes.Axes.plot的其他参数
    '''
    def __init__(self, x, y=None, **kwargs):
        if y is None:
            self._x = np.arange(len(x))
            self._y = x
        else:
            self._x = x
            self._y = y
        self._kwargs = kwargs

    def __call__(self, axes):
        '''
        主绘图函数

        Parameter
        ---------
        axes: matplotlib.axes.Axes
            需要绘制的子图
        '''
        axes.plot(self._x, self._y, **self._kwargs)

class BarPlot(PlotComponentBase):
    '''
    条形图

    Parameter
    ---------
    data: np.array or the like
        每个标签对应的数据的高度
    x: np.array, default None
        标签的位置，如果为None，则自动生成为numpy.arange(len(data))
    vertical: boolean, default True
        是否为纵向条形图，如果为False，将绘制横向条形图
    kwargs: dictionary
        其他需要传入到bar或者barh中的参数
    '''
    def __init__(self, data, x=None, vertical=True, **kwargs):
        if x is None:
            self._x = np.arange(len(data))
        else:
            self._x = x
        self._data = data
        if vertical:
            self._func_name = 'bar'
        else:
            self._func_name = 'barh'
        self._kwargs = kwargs

    def __call__(self, axes):
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
    '''
    def __init__(self, open_price, close_price, high_price, low_price,
                 color_up='red', color_down='green', alpha=.75, width=0.5):
        self._data = (open_price, high_price, low_price, close_price)
        self._color_up = color_up
        self._color_down = color_down
        self._alpha = alpha
        self._width = width

    def __call__(self, axes):
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
    **kwargs: dictionary
        其他传入到hist中的参数
    '''
    def __init__(self, data, bins, **kwargs):
        self._data = data
        self._bins = bins
        self._kwargs = kwargs

    def __call__(self, axes):
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
    kwargs: dictionary
        其他传入到matplotlib.axes.Axes.stackplot中的参数，仅通过关键字设置
    '''
    def __init__(self, x, *y, labels=None, colors=None, **kwargs):
        self._x = x
        self._y = y
        self._kwargs = kwargs
        if labels is not None:
            self._kwargs.update(labels=labels)
        if colors is not None:
            self._kwargs.update(colors=colors)

    def __call__(self, axes):
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
    kwargs: dictionary
        其他传入到matplotlib.axes.Axes.scatter中的参数
    '''
    def __init__(self, x, y=None, **kwargs):
        if y is None:
            self._y = x
            self._x = np.arange(len(x))
        else:
            self._x = x
            self._y = y
        self._kwargs = kwargs

    def __call__(self, axes):
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
        # 使用深复制FontProperties对象保证对字体大小的修改
        self._font_properties = font_properties
        if font_size is not None:
            self._font_properties = self._font_properties.copy()
            self._font_properties.set_size(font_size)
        self._kwargs = kwargs

    def __call__(self, axes):
        axes.set_title(self._text, fontproperties=self._font_properties, **self._kwargs)

# --------------------------------------------------------------------------------------------------
# legend
class LegendComponent(PlotComponentBase):
    '''
    仅一个简单的legend开关，因为legend需要与line等对象配合使用，使用handler和label的机制
    会导致相关功能和设计的复杂化，因此暂时不采用这种设计
    使用该组件，要求在各种plot中设置label参数

    Parameter
    ---------
    loc: string, default 'upper right'
        legend所在的位置
    font_properties: matplotlib.font_manager.FontProperties, default HT_FONT
        字体设置
    font_size: int, default None
        字体大小
    kwargs: dictionary
        其他传入到axes.legend的参数
    '''
    def __init__(self, loc='upper right', font_properties=HT_FONT, font_size=None, **kwargs):
        self._loc = loc
        self._font_properties = font_properties
        if font_size is not None:
            self._font_properties = self._font_properties.copy()
            self._font_properties.set_size(font_size)
        self._kwargs = kwargs

    def __call__(self, axes):
        axes.legend(loc=self._loc, prop=self._font_properties, **self._kwargs)

# --------------------------------------------------------------------------------------------------
# annotate
class AnnotateComponent(PlotComponentBase):
    '''
    注解工具

    Parameter
    ---------
    text: string
        注解内容
    xy: iterable
        指向的坐标，必须是二维
    font_properties: matplotlib.font_manager.FontProperties, default HT_FONT
        字体设置
    font_size: int, default None
        字体大小设置
    xytext: iterable, optional, default None
        注解文字所在的坐标，必须是二维，默认为xy
    xycoords: string, Artist and the like, optional, default None
        指向坐标系单位，详见https://matplotlib.org/api/_as_gen/matplotlib.axes.Axes.annotate.html#matplotlib.axes.Axes.annotate
    textcoords: string, Artist and the like, optional, default None
        注解文字坐标系单位
    arrowprop: dictionary, optional, default None
        箭头设置属性
    annotation_clip: boolean, optional, default None
        注解是否可见，如果为True，则只有当xy在axes范围内时才可见；反之，任何条件下均可见
    kwargs: dictionary
        其他传入到Text中的参数
    '''
    def __init__(self, text, xy, font_properties=HT_FONT, font_size=None, xytext=None,
                 xycoords=None, textcoords=None, arrowprops=None, annotation_clip=None,
                 **kwargs):
        self._text = text
        self._xy = xy
        self._font_properties = font_properties
        if font_size is not None:
            self._font_properties = self._font_properties.copy()
            self._font_properties.set_size(font_size)
        self._annotate_kwargs = self._set_annotate_kwargs(xytext=xytext, xycoords=xycoords,
                                                          textcoords=textcoords, arrowprops=arrowprops,
                                                          annotation_clip=annotation_clip)
        self._kwargs = kwargs

    def __call__(self, axes):
        axes.annotate(self._text, self._xy, fontproperties=self._font_properties, **self._annotate_kwargs,
                      **self._kwargs)

    def _set_annotate_kwargs(self, **kwargs):
        '''
        将要传入到axes.annotate中的相关参数进行更新
        更新的规则如下：
        如果kwargs中的值为None，则不添加到annotate_kwargs中，如果为非None
        则直接在annotate_kwargs中进行更新

        Parameter
        ---------
        kwargs: dictionary
            参数的具体设置

        Return
        ------
        annotate_kwargs: dictionary
        '''
        result = {k: kwargs[k] for k in kwargs if kwargs[k] is not None}
        return result

# --------------------------------------------------------------------------------------------------
# Axis
class AxisComponent(PlotComponentBase):
    '''
    坐标轴处理类
    主要用于处理坐标轴的范围、坐标轴的label、坐标标示等
    内部完全采用set_ticks和set_ticklabels来实现

    Parameter
    ---------
    major_tick_pos: iterable
        主刻度的位置
    major_tick_labels: iterable
        主刻度标签，长度与major_tick_pos一致
    minor_tick_pos: iterable, default None
        副刻度的位置
    ticklabel_font: matplotlib.font_manager.FontProperties, default HT_FONT
        刻度字体
    ticklabel_fontsize: int, default None
        刻度字体大小
    ticklabel_kwargs: dictionary, default None
        其他刻度设置，用于传入到axis.set_ticklabels
    label_text: string, default None
        轴的标签
    label_font: matplotlib.font_manager.FontProperties, default HT_FONT
        轴标签的字体
    label_fontsize: int, default None
        轴标签的字体大小
    label_kwargs: dictionary, default None
        其他轴标签设置，用于传入到axis.set_label_text
    enable_grid: boolean, default False
        是否启用grid
    grid_kwargs: dictionary, default None
        其他传入axis.grid中的参数
    '''
    def __init__(self, major_tick_pos, major_tick_labels, minor_tick_pos=None,
                 ticklabel_font=HT_FONT, ticklabel_fontsize=None, ticklabel_kwargs=None,
                 label_text=None, label_font=HT_FONT, label_fontsize=None, label_kwargs=None, enable_grid=False,
                 grid_kwargs=None):
        self._major_tick_pos = major_tick_pos
        self._major_tick_labels = major_tick_labels
        self._minor_tick_pos = minor_tick_pos
        if self._minor_tick_pos is not None:
            self._minor_tick_labels = ['' for dummy_i in range(len(self._minor_tick_pos))]
        else:
            self._minor_tick_labels = None

        self._ticklabel_fontproperties = ticklabel_font
        if ticklabel_fontsize is not None:
            self._ticklabel_fontproperties = self._ticklabel_fontproperties.copy()
            self._ticklabel_fontproperties.set_size(ticklabel_fontsize)
        if ticklabel_kwargs is None:
            self._ticklabel_kwargs = {}
        else:
            self._ticklabel_kwargs = ticklabel_kwargs

        self._label_text = label_text
        self._label_fontproperties = label_font
        if label_fontsize is not None:
            self._label_fontproperties = self._label_fontproperties.copy()
            self._label_fontproperties.set_size(label_fontsize)
        if label_kwargs is None:
            self._label_kwargs = {}
        else:
            self._label_kwargs = label_kwargs

        self._endable_grid = enable_grid
        if grid_kwargs is None:
            self._grid_kwargs = {}
        else:
            self._grid_kwargs = grid_kwargs


    def _set_tick(self, axis, minor=False):
        '''
        设置刻度轴，涉及刻度轴定位和标签格式

        Parameter
        ---------
        axis: matplot.axis.Axis
            需要修改的坐标轴对象
        minor: boolean
            是否设置副刻度轴
        '''
        if minor:
            if self._minor_tick_pos is None:
                raise ValueError('Minor tick is not specified!')
            tick_type = 'minor'
        else:
            tick_type = 'major'
        pos = getattr(self, '_{}_tick_pos'.format(tick_type))
        labels = getattr(self, '_{}_tick_labels'.format(tick_type))
        axis.set_ticks(pos, minor=minor)
        if not minor:
            axis.set_ticklabels(labels, minor=False, fontproperties=self._ticklabel_fontproperties,
                                **self._ticklabel_kwargs)

    def __call__(self, axis):
        self._set_tick(axis, False)
        if self._minor_tick_pos is not None:
            self._set_tick(axis, True)
        # 设置轴标签
        if self._label_text is not None:
            axis.set_label_text(self._label_text, fontproperties=self._label_fontproperties,
                                **self._label_kwargs)
        # 设置grid
        axis.grid(self._endable_grid, **self._grid_kwargs)

def num_multiple_ticker(vmin, vmax, base, formatter):
    '''
    生成处于给定数据区间，且是某个数据倍数的刻度位置及标签

    Parameter
    ---------
    vmin: float
        下边界
    vmax: float
        上边界
    base: float
        基数
    formatter: string
        使用formatter.format(x)获取标签

    Return
    ------
    pos: list
        刻度位置
    labels: list
        刻度标签
    '''
    vmin = vmin // base * base
    vmax = (vmax // base + 1) * base
    n = (vmax - vmin) // base + 1
    pos = vmin + np.arange(n) * base
    labels = [formatter.format(x) for x in pos]
    return pos, labels

def date_ticker(dates, locator, formatter='{:%Y-%m-%d}', add_header=True, add_tail=False):
    '''
    生成给定日期序列的刻度位置及标签

    Parameter
    ---------
    dates: iterable
        元素为datetime及其子类
    locator: function(date)->boolean
        返回True表示以该点为刻度
    formatter: string, default '{:%Y-%m-%d}'
        使用formatter.format(date)获取对应刻度的标签
    add_header: boolean, default True
        是否加入第一个日期
    add_tail: boolean, default False
        是否加入最后一个日期

    Return
    ------
    pos: list
        刻度位置，元素为int
    labels: list
        刻度标签，元素为string
    '''
    pos = [i for i, d in enumerate(dates) if locator(d)]
    if add_header and 0 not in pos:
        pos = [0] + pos
    last_pos = len(dates) - 1
    if add_tail and last_pos not in pos:
        pos.append(last_pos)
    labels = [formatter.format(dates[i]) for i in pos]
    return pos, labels

# --------------------------------------------------------------------------------------------------
# 其他组件
class AxisLimitSetter(PlotComponentBase):
    '''
    用于设置坐标轴的范围

    Parameter
    ---------
    vmin: float
        坐标轴最小值
    vmax: float
        坐标轴最大值
    axis_name: string
        设置的坐标轴名称，默认为x，仅可以为[x, y]

    Notes
    -----
    参数输入的顺序无关
    '''
    def __init__(self, vmin, vmax, axis_name='x'):
        if vmin > vmax:
            self._limit = (vmax, vmin)
        else:
            self._limit = (vmin, vmax)
        valid_axises = ['x', 'y']
        if axis_name not in valid_axises:
            raise ValueError('Invalid axis name(you provide {yp}, valids are {v}).'.
                             format(yp=axis_name, v=valid_axises))
        self._setter_func = 'set_{}lim'.format(axis_name)

    def __call__(self, axes):
        getattr(axes, self._setter_func)(self._limit)

# --------------------------------------------------------------------------------------------------
# 其他工具
def secondary_axis_wrapper(axis_type):
    '''
    axis_type: string
        共享轴的类型，仅支持[x, y]
    '''
    def wrapper(func):
        @wraps(func)
        def inner(axes, *args, **kwargs):
            axes = getattr(axes, 'twin{}'.format(axis_type))()
            result = func(axes, *args, **kwargs)
            return result
        return inner
    return wrapper

