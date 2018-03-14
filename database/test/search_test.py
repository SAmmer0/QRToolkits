#!/usr/bin/env python
# -*- coding:utf-8
"""
Author:  Hao Li
Email: howardleeh@gmail.com
Github: https://github.com/SAmmer0
Created: 2018/3/8
"""
from shutil import rmtree
import os.path as os_path
from pprint import pprint

import fmanager
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from database.db import Database

test_factors = [{'name': 'ZX_IND',
                 'store format': (DataClassification.STRUCTURED, DataValueCategory.CHAR, DataFormatCategory.PANEL),
                 'dtype': None,
                 'rel_path': 'ind.zx'},
                {'name': 'CLOSE',
                 'store format': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                 'dtype': 'float64',
                 'rel_path': 'quote.close'},
                {'name': 'ADJ_CLOSE',
                 'store format': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                 'dtype': 'float64',
                 'rel_path': 'quote.adj_close'},
                {'name': 'BETA',
                 'store format': (DataClassification.STRUCTURED, DataValueCategory.NUMERIC, DataFormatCategory.PANEL),
                 'dtype': 'float64',
                 'rel_path': 'basicfactor.beta'}]
start_time = '2014-01-01'
end_time = '2018-03-01'
db_path = r'C:\Users\c\Desktop\test\db_test'

if os_path.exists(db_path):
    rmtree(db_path)
    rmtree(r'C:\Users\c\Documents\DatabaseMetadata')

db = Database(db_path)
for factor in test_factors:
    factor_data = fmanager.query(factor['name'], (start_time, end_time))
    result = db.insert(factor_data, factor['rel_path'], factor['store format'], factor['dtype'])
    print(result)

unstruct_data = list(range(1000))
print(db.insert(unstruct_data, 'unstruct_data.test', (DataClassification.UNSTRUCTURED, )))

db.print_collections()
pprint(db.find_data('beta'))
print(db.find_collection('quote'))

db.remove_data('ind.zx', test_factors[0]['store format'])

db.print_collections()

db.move_to('basicfactor.beta', 'quote.beta', test_factors[3]['store format'])

db.print_collections()
