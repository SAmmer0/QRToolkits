# -*- encoding: utf-8

from database.jsonEngine.dbcore import date2filename, date2filenamelist, DB_CONFIG

# 手动变换配置测试不同的效果
DB_CONFIG['data_spilt_frequency'] = 'YEAR'

assert date2filename('2018-01-01') == '2018'
assert date2filenamelist('2017-01-01', '2018-02-01') == ['2017', '2018']

DB_CONFIG['data_spilt_frequency'] = 'QUARTER'
assert date2filename('2018-02-01') == '2018Q1'
assert date2filenamelist('2016-06-03', '2018-01-03') == ['2016Q2', '2016Q3', '2016Q4', '2017Q1', '2017Q2', '2017Q3', '2017Q4', '2018Q1']

DB_CONFIG['data_spilt_frequency'] = 'MONTH'
assert date2filename('2018-02-01') == '201802'
assert date2filenamelist('2017-11-03', '2018-02-01') == ['201711', '201712', '201801', '201802']

print('Test done!')
