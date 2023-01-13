import time
import logging
import os
import sys

from builder.builder import Builder

if __name__ == '__main__':
    schema = "real_table"

    start_time = time.time()
    print('Init: Import_Data')
    path = 'baslake/data/MMA_FULL.pkl'
    builder = Builder(schema, path)
    entities, complex_keys = builder.process()
    end_time = time.time() - start_time
    print('Finish: Import_Data | Duration: %s' % end_time)
