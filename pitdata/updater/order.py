#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/18
"""
from collections import deque

class DependencyNode(object):
    '''
    数据依赖节点类，用于表示数据计算之间的依赖关系
    一个节点可以依赖多个(或者0个)其他节点，同时该节点也可以被多个其他节点依赖
    每个节点都维护其所依赖的节点以及依赖其的节点

    Parameter
    ---------
    name: string
        节点名称，通常为数据名称，是该节点的唯一标识
    '''
    def __init__(self, name):
        self.name = name
        self._parents = []    # 依赖项(即没有母节点不会有子节点)
        self._children = []    # 被依赖项(即其他节点存在的前提)

    def add_parent(self, parent):
        '''
        添加依赖项节点

        Parameter
        ---------
        parent: DependencyNode
        '''
        self._parents.append(parent)
        parent.add_child(self)

    def add_child(self, child):
        '''
        添加被依赖的节点

        Parameter
        ---------
        child: DependencyNode
        '''
        self._children.append(child)

    def get_descendant(self):
        '''
        获取所有对该节点具有直接或者间接依赖的节点

        Return
        ------
        out: list
            元素为节点对象
        '''
        queue = deque()
        queue.append(self)
        out = []
        while len(queue) > 0:
            node = queue.pop()
            for child in node._children:
                if child not in out:
                    out.append(child)
                queue.append(child)
        return out

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name) | hash(1)    # 添加1避免与对应的字符串具有相同哈希


