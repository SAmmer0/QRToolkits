#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/13

各种常用布局模板，为了隐藏基类中复杂的接口，采用has-a模式，而不是is-a模式
当前提供的模板包含：
n*k方格图
n行图
"""
from abc import ABCMeta, abstractclassmethod
from math import ceil

from plottools.layout.basiclayout import GridLayoutBase

class LayoutTemplateBase(object, metaclass=ABCMeta):
    '''
    布局模板基类
    主要提供以下接口：
    get_next_axes：获取下一个图的axes
    figure：获取当前的Figure对象
    current_axes：获取当前的Axes对象
    axes：获取该布局下所生成的所有Axes对象
    
    Parameter
    ---------
    nrow: int
        行数
    ncol: int
        列数
    cell_width: float, default 4
        每个方格的宽度
    cell_height: float, default 4
        每个方格的高度
    '''
    def __init__(self, nrow, ncol, cell_width=4, cell_height=4):
        self._glb = GridLayoutBase(nrow, ncol, cell_width, cell_height)
    
    @abstractclassmethod
    def get_next_axes(self):
        '''
        获取下一个图的Axes

        Return
        ------
        axes: matplotlib.axes.Axes
            如果没有下一个图，则返回None
        '''
        return None
    
    def tight_layout(self, **kwargs):
        '''
        使子图布局更为合理，避免子图之间出现重叠，一般在所有子图绘制完成后调用

        Parameter
        ---------
        kwargs: dictionary
            传入到GridSpec.tight_layout(fig)中的其他参数
        '''
        self._glb.tight_layout(**kwargs)

    @property
    def figure(self):
        return self._glb.figure

    @property
    def axes(self):
        return self._glb.axes

    @property
    def current_axes(self):
        return self._glb.current_axes
    

class NKLayout(LayoutTemplateBase):
    '''
    N*K方格图

    Parameter
    ---------
    nrow: int
        行数
    ncol: int
        列数
    cell_width: float, default 4
        每个方格的宽度
    cell_height: float, default 4
        每个方格的高度
    '''
    def get_next_axes(self):
        '''
        获取下一个图的Axes

        Return
        ------
        axes: matplotlib.axes.Axes
            如果没有下一个图，则返回None
        '''
        return self._glb.get_next_axes(1, 1)
    
class GeneralNLineLayout(LayoutTemplateBase):
    '''
    通用型N行图，即每行的高度可以不同
    
    Parameter
    ---------
    row_list: iterable
        元素为每行的高度，必须为正整数，非整数会被强制向上转换为整数，每行的实际高度为row*cell_height
    cell_height: float, default 1
        1个方格单位的高度
    cell_width: float, default 4
        每行的宽度
    '''
    def __init__(self, row_list, cell_height=1, cell_width=4):
        self._row_list = self._trans_rows(row_list)
        self._current_axes_idx = 0
        super().__init__(sum(self._row_list), 1, cell_width, cell_height)
    
    def _trans_rows(self, row_list):
        '''
        检测行高参数是否均为正整数，如果存在负数则报错，对于非整数，使用ceil函数转换为整数
        
        Parameter
        ---------
        row_list: iterable
            元素为每行高度
        
        Return
        ------
        out: list
            转换后的行高
        '''
        for row in row_list:
            if row <= 0:
                raise ValueError('Row number should be positive!')
        out = [ceil(x) for x in row_list]
        return out
    
    def get_next_axes(self):
        '''
        获取下一个图的Axes

        Return
        ------
        axes: matplotlib.axes.Axes
            如果没有下一个图，则返回None
        '''
        if self._current_axes_idx >= len(self._row_list):
            return None
        ax = self._glb.get_next_axes(self._row_list[self._current_axes_idx], 1)
        self._current_axes_idx += 1
        return ax

class NLineLayout(GeneralNLineLayout):
    '''
    N行图，每行高度相同
    
    Parameter
    ---------
    nrow: int
        行数
    cell_height: float, default 4
        每个方格的高度
    wh_ratio: flaot, 2
        每行图的宽和高的比率
    '''
    def __init__(self, nrow, cell_height=4, wh_ratio=2):
        super().__init__([1 for dummy_i in range(nrow)], cell_height, wh_ratio*cell_height)
