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
    query = f"""
        SELECT 
            c.TABLE_SCHEMA          as table_schema
            ,c.TABLE_NAME           as table_name
            ,COLUMN_NAME            as column_name
            ,DATA_TYPE              as data_type
            ,ISNULL(CHARACTER_MAXIMUM_LENGTH, -1)   as character_length
            ,CASE
                WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE + '(max)'
                WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE +
                    '(' + CAST(CHARACTER_MAXIMUM_LENGTH as VARCHAR(15)) + ')'
                WHEN DATA_TYPE = 'decimal' THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE +
                    '(' + CAST(NUMERIC_PRECISION as VARCHAR(max)) + ',' + CAST(NUMERIC_SCALE as VARCHAR(MAX)) + ')'
                ELSE
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE
            END as composite_name
        FROM 
            INFORMATION_SCHEMA.COLUMNS as c, INFORMATION_SCHEMA.SCHEMA as t
            
        WHERE 
            TABLE_TYPE = 'BASE TABLE' AND c.Table_Schema = t.Table_Schema AND c.Table_Name and t.TABLE_NAME AND
            c.Table_Schema = '{schema}' AND c.TABLE_NAME = '{table}'
        ORDER BY c.Table_Schema, c.TABLE_NAME
    """
    with pypyodbc.connect(connect_str) as conn:
        output_df = pd.read_sql(query, conn)
        if fields:
            # recent pandas version would allow for output_df.astype(fields)
            for field, field_type in fields.items():
                output_df[field] = output_df[field].astype(field_type)
        return output_df


def bulk_insert(df, conn_str, schema, table, pre_insert_query=None, chunks=10**4, primary_keys=None, identity=False,
                identity_name='ID', execute_many=True):
    """
    Function to insert data in bulk-chunks. If a delete in the table is required, the corresponding `pre_insert_query`
    is executed before the insert.

    :param df: pandas.DataFrame
    :param conn_str: connection_string
    :param schema: database.schema
    :param table: database.schema.table (only table)
    :param pre_insert_query: query to be executed before the insert
    :param chunks: number of rows to be inserted before the insert
    :param primary_keys: primary keys of the table
    :param identity: whether ID column is to be inc
    :param identity_name:
    :param execute_many:
    :return:
    """
    ...


