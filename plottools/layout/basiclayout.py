#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/12
"""

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt

class GridLayoutBase(object):
    '''
    图片布局基类，对matplotlib.gridspec模块进行包装，提供基本的绘图布局功能
    布局要求不能有任何图跨行，仅能跨列

    Parameter
    ---------
    total_row_num: int
        整个图中包含的行数
    total_col_num: int
        整个图中包含的列数
    single_col_width: int
        每列的宽度
    single_col_width: int
        每行的高度
    '''
    def __init__(self, total_row_num, total_col_num, single_col_width, single_row_height):
        self._total_row_num = total_row_num
        self._total_col_num = total_col_num
        self._figure = plt.figure(figsize=(total_col_num * single_col_width, total_row_num * single_row_height))
        self._axes = []
        self._gs = gridspec.GridSpec(total_row_num, total_col_num)
        self._current_row = None
        self._current_col = None
        self._current_row_gap = 1   # 当前图跨行的数量
        self._current_col_gap = 1   # 当前图跨列的数量

    def get_next_row_head(self, row_gap, col_gap):
        '''
        获取下一行首图的Axes

        Parameter
        ---------
        row_gap: int
            该图跨行的数量
        col_gap: int
            该图跨列的数量

        Return
        ------
        axes: matplotlib.axes.Axes
        '''
        if self._is_last_row():     # 当前已经为最后一行
            return None

        self._current_row = self._get_next_row_id()
        self._current_col = 0
        row_end, self._current_row_gap = self._normalize_axes(self._current_row, row_gap,
                                                              self._total_row_num, 'vertical')
        col_end, self._current_col_gap = self._normalize_axes(self._current_col, col_gap,
                                                              self._total_col_num, 'horizontal')
        axes = self._figure.add_subplot(self._gs[self._current_row: row_end, self._current_col: col_end])
        self._axes.append(axes)
        self._gs.tight_layout(self._figure)
        return axes

    def get_next_col(self, col_gap):
        '''
        获取本行中下一列图的Axes，如果仅用于获取本行非首图

        Parameter
        ---------
        col_gap: int
            该图跨列的数量

        Return
        ------
        axes: matplotlib.axes.Axes
        '''
        if self._is_last_col() or self._current_row is None:
            return None
        self._current_col = self._get_next_col_id()
        col_end, self._current_col_gap = self._normalize_axes(self._current_col, col_gap,
                                                              self._total_col_num, 'horizontal')
        row_end = self._get_next_row_id()
        axes = self._figure.add_subplot(self._gs[self._current_row: row_end, self._current_col: col_end])
        self._axes.append(axes)
        self._gs.tight_layout(self._figure)
        return axes

    def get_next_axes(self, row_gap, col_gap):
        '''
        获取下一个图的Axes

        Parameter
        ---------
        row_gap: int
            该图跨行数量，如果当前行已经有行首，则该参数无效
        col_gap: int
            该图跨列数量

        Return
        ------
        axes: matplotlib.axes.Axes
        '''
        if self._current_row is None or self._is_last_col():
            axes = self.get_next_row_head(row_gap, col_gap)
        else:
            axes = self.get_next_col(col_gap)
        return axes

    @staticmethod
    def _normalize_axes(current_pos, gap, boundary, norm_type):
        '''
        判断给定的行或者列的跨度是否超出最大跨度，如果超出，则将其限制在最大跨度内

        Parameter
        ---------
        current_pos: int
            当前图片位置
        gap: int
            当前图片占位
        boundary:
            边界位置
        norm_type: string
            边界类型，仅支持['vertical', 'horizontal']，分别表示纵向边界，横向边界

        Return
        ------
        exp_boundry: int
            正则化后的边界
        gap: int
            正则化后的图片占位
        '''
        supported_norm_type = ['vertical', 'horizontal']
        if norm_type not in supported_norm_type:
            raise ValueError('Invalid norm_type, only support {sp}, you provided {yp}'.\
                             format(sp=supported_norm_type, yp=norm_type))
        exp_boundary = current_pos + gap
        if exp_boundary> boundary:
            exp_boundary = boundary
            # 此处添加日志
        return exp_boundary, exp_boundary - current_pos

    def _is_last_row(self):
        '''
        判断当axes是否是最后一行

        Return
        ------
        out: boolean
        '''
        return self._get_next_row_id() >= self._total_row_num

    def _is_last_col(self):
        '''
        判断当前axes是否是最后一列

        Return
        ------
        out: boolean
        '''
        return self._get_next_col_id() >= self._total_col_num

    def _get_next_row_id(self):
        '''
        计算下一行的行号索引

        Return
        ------
        row: int
        '''
        result = 0
        if self._current_row is not None:
            result = self._current_row + self._current_row_gap
        return result

    def _get_next_col_id(self):
        '''
        计算下一列的列号索引

        Return
        ------
        col: int
        '''
        result = 0
        if self._current_col is not None:
            result = self._current_col + self._current_col_gap
        return result
