#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/4/20
"""
from pitdata.updater.loader import load_all

# 在执行下面代码前，先修改配置文件到指定的包含数据计算方法脚本的文件夹
# 如果文件夹为空，则需要添加至少一个数据计算文件脚本

res = load_all()
