# -*- encoding: utf-8

from database.hdf5Engine.dbcore import HDF5Engine
from database.db import ParamsParser
from database.const import DataClassification, DataValueCategory, DataFormatCategory

db_path = r'C:\Users\c\Desktop\test'
rel_path = 'test_remove'

result = HDF5Engine.remove_data(ParamsParser.from_dict(db_path, {'rel_path': rel_path,
                                                        'store_fmt': (DataClassification.STRUCTURED, 
                                                                      DataValueCategory.NUMERIC,
                                                                      DataFormatCategory.PANEL)}))
print(result)
