#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/7/27
"""
import matplotlib.pyplot as plt
from plottools.plot_helper.font_helper import chinese_font_context

def main_tester(func):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    func(ax)

def context_test(ax):
    with chinese_font_context():
        ax.set_title('这是一个测试')
        ax.set_xlabel('x轴标签测试')
        ax.set_ylabel('y轴标签测试(-100)')

def rcparam_test(ax):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    ax.set_title('这是一个测试')
    ax.set_xlabel('x轴标签测试')
    ax.set_ylabel('y轴标签测试')

if __name__ == '__main__':
    # main_tester(rcparam_test)
    main_tester(context_test)
