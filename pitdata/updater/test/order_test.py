#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/19
"""
from pitdata.updater.order import DependencyNode, DependencyTree

# --------------------------------------------------------------------------------------------------
# case1
case1 = {'good1': ['good2', 'good3'], 'good2': None, 'good3': ['good2'], 'good4': None}
tree1 = DependencyTree(case1, lambda x: x)
print(tree1.generate_dependency_order())
print(tree1.get_branch('good3'))
print(tree1.get_branch('good2'))

# --------------------------------------------------------------------------------------------------
# case2
case2 = {'good1': ['good5', 'good3'], 'good2': None, 'good3': ['good2'], 'good4': None, 'good5': ['good2']}
tree2 = DependencyTree(case2, lambda x: x)
print(tree2.generate_dependency_order())
print(tree2.get_branch('good3'))
print(tree2.get_branch('good2'))

# --------------------------------------------------------------------------------------------------
# case3
case3 = {'good1': ['good5', 'good3'], 'good2': None, 'good3': ['good2'], 'good4': None, 'good5': ['good4']}
tree3 = DependencyTree(case3, lambda x: x)
print(tree3.generate_dependency_order())
print(tree3.get_branch('good3'))
print(tree3.get_branch('good2'))
