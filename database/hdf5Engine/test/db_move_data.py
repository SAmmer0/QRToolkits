# -*- encoding: utf-8

from database.hdf5Engine.dbcore import HDF5Engine
from database.const import DataClassification, DataFormatCategory, DataValueCategory
from database.db import ParamsParser

db_path = r'C:\Users\c\Desktop\test'
src_path = 'test_move_src'
dest_path = 'test_move_dest'
src_params = ParamsParser.from_dict(db_path, {'rel_path': src_path, 
                                             'store_fmt': (DataClassification.STRUCTURED,
                                                           DataValueCategory.NUMERIC,
                                                           DataFormatCategory.PANEL)})
dest_params = ParamsParser.from_dict(db_path, {'rel_path':dest_path,
                                               'store_fmt': (DataClassification.STRUCTURED,
                                                           DataValueCategory.NUMERIC,
                                                           DataFormatCategory.PANEL)})
result = HDF5Engine.move_to(src_params, dest_params)
print(result)
