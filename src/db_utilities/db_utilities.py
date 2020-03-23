import time
import logging
import datetime
import numpy as np
import pandas as pd
import sys

from types import MappingProxyType

import pypyodbc


def get_table_details(schema, table, connect_str):
    """
    Returns the characteristics of the table 'table' within the schema 'schema'
    @param schema: schema
    @param table: table
    @param connect_str: connection string to be used to database connection
    @return: DataFrame containing the characteristics of each column of the table
    TODO: Finish query
    """

    fields = {'character_length': 'int32'}
    query = """
            SELECT c.TABLE_SCHEMA   as table_schema,
            c.TABLE_NAME            as table_name


    """
    with pypyodbc.connect(connect_str) as conn:
        output_df = pd.read_sql(query, conn)
        if fields:
            # recent pandas version would allow for output_df.astype(fields)
            for field, field_type in fields.items():
                output_df[field] = output_df[field].astype(field_type)
        return output_df


