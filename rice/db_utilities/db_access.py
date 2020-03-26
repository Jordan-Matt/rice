import sys
import pandas as pd
import datetime
import logging
from ..data_blend import df_astype
# from .data_blend import df_astype


def get_connection(conn_str, db_type, library=None):
    """
    Opens a connection to a database.
    @param conn_str:
    @param db_type:
    @param library:
    @return:
    """
    conn_lib = None
    library = {
        'sqlserver': 'pyodbc',
        'postgresql': 'psypg2',
        'oracle': 'cx_Oracle'
    }.get(db_type) if not library else library

    if db_type == 'sqlserver':
        if library == 'pypyodbc':
            try:
                import pypyodbc as conn_lib
            except ImportError:
                print("You must have pypyodbc installed. Run pip install pypyodbc.")
                sys.exit(1)
        elif library == 'pyodbc':
            try:
                import pyodbc as conn_lib
            except ImportError:
                logging.warning("pyodbc not found. Falling back to pypyodbc")
                try:
                    import pypyodbc as conn_lib
                except ImportError:
                    print("You must have pypyodbc installed. Run pip install pypyodbc.")
                    sys.exit(1)
    elif db_type == 'postgresql':
        if library == 'psypg2':
            print('We havent set up support for postgresql')
    elif db_type == 'oraclesql':
        print('We havent set up support for Oracle')

    if not conn_lib:
        raise ValueError(f'Invalid db_type/library combo. db_type={db_type}, library={library}')
    return conn_lib.connect(conn_str)


def query_sql_pandas(sql_req, conn_str, db_type='sqlserver', dtypes=None, fill_na=None, library=None,
                     logger=logging.info):
    """
    Connects to the database, puts all of the row data into a Pandas table, close connection, and returns the pandas
    dataframe.

    @param sql_req: sql query
    @param conn_str: connection_string
    @param db_type: type of database, default = sqlserver
    @param dtypes: assign data types to columns
    @param fill_na: fills None values in DataFrame.
    @param library: library to use for provided db_type
    @return: DataFrame (from the server)
    """
    logger('Running SQL Query: ' + ' '.join(sql_req.split()))

    start_time = datetime.datetime.now()
    connection = get_connection(conn_str, db_type, library)

    try:
        data = pd.read_sql(sql_req, connection)
        if fill_na:
            data.fillna(value=fill_na, inplace=True)
        if dtypes:
            data = df_astype(data, dtypes)

    except Exception as err:
        logging.exception(f'[{__name__}] Error reading SQL query: {err} - Time Took: '
                          f'{(datetime.datetime.now() - start_time).total_seconds()} seconds.')

        if db_type == 'sqlserver' and library != 'pypyodbc':
            logger('Using fallback library pypyodbc')
            return query_sql_pandas(sql_req, conn_str, db_type, dtypes, fill_na, library='pypyodbc')
        return pd.DataFrame()

    finally:
        connection.close()

    end_time = datetime.datetime.now()
    logger(f'Retreived {len(data)} rows in {(end_time - start_time).total_seconds()} seconds.')
    return data
