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

    @property
    def parents(self):
        return self._parents

    def __str__(self):
        return '<DependencyNode: {}>'.format(self.name)

    def __repr__(self):
        return 'DependencyNode(name={})'.format(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name) | hash(1)    # 添加1避免与对应的字符串具有相同哈希


class DependencyTree(object):
    '''
    依赖树类，用于根据数据的依赖关系构建完整的依赖树，并对依赖树进行相关操作

    Parameter
    ---------
    dep_data: dict
        原始描述依赖关系的数据，格式为{name: dep}，其中dep的形式可以为任意，但必须包含数据之间的依赖信息
    func: function, default None
        用于从dep_data中提取依赖关系的函数，要求格式为function(dep)->iterable(若无依赖则返回None)，默认情况下的该函数为从
        pitdata.updater.loader.load_all中提取依赖信息
    '''
    def __init__(self, dep_data, func=None):
        self._dep_tree = {n: DependencyNode(n) for n in dep_data}
        if func is None:
            func = lambda x: x['data_description'].dependency
        for d in self._dep_tree:
            dep = func(dep_data[d])
            if dep is not None:
                for dp in dep:
                    self._dep_tree[d].add_parent(self._dep_tree[dp])

    def generate_dependency_order(self):
        '''
        根据数据之间的依赖关系生成数据的顺序，最终顺序保证任何一个节点的位置一定在其依赖的节点之后，
        无依赖关系的情况下按照节点的名称排序

        Return
        ------
        order: list
        '''
        dep_data = sorted(self._dep_tree.values(), key=lambda x: x.name)
        order = []
        def add_node(node, container):
            # 先递归添加该节点的依赖节点，再添加该节点
            if node not in container:
                for p in node.parents:
                    add_node(p, container)
                container.append(node)
        for node in dep_data:
            add_node(node, order)
        return order

    def get_branch(self, name):
        '''
        获取所有对给定节点具有(直接或者间接)依赖的数据的名称(包括该节点)

        Parameter
        ---------
        name: string
            节点的名称

        Return
        ------
        out: list
        '''
        out = self._dep_tree[name].get_descendant()
        out = [d.name for d in out]
        out.append(name)
        return out
