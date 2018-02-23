# -*- encoding: utf-8
'''
对dbcore中DataWrapper的功能进行测试
'''

import json

from fmanager import query
from database.jsonEngine.dbcore import DataWrapper

sample_data = query('ZX_IND', ('2017-12-01', '2018-02-01'))
sample_data2 = query('ZX_IND', ('2017-11-01', '2018-01-01'))

# 从pandas数据初始化
data = DataWrapper.init_from_pd(sample_data)
data2 = DataWrapper.init_from_pd(sample_data2)

print('Done!')
