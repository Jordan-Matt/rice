import os
import logging
import fastavro
import pandas as pd
# print(pd.__version__)


def read_avro(path):
    """
    Reads the avro file in argument and returns pandas.DataFrame
    @param path: filepath
    @return: pandas.DataFrame
    """
    with open(path, 'rb') as f:
        reader = fastavro.reader(f)
        records = [r for r in reader]
        return pd.DataFrame.from_records(records)


def read_avro_blocks(path, logger=None):
    """
    Reads the avro file in argument and returns an iterator
    @param path: full path of the avro file to read
    @return: avro blocks iterator
    """
    if not os.path.exists(path):
        if logger:
            logger.error(f'No file found: {path}')
        else:
            print(f'No file found: {path}')

    with open(path, 'rb') as f:
        reader = fastavro.block_reader(f)
        for block in reader:
            yield block

