from sqlalchemy import create_engine, MetaData, Table
from os import getenv
from pandas import read_sql_query

from pandas import DataFrame
from typing import Literal
from sqlalchemy import Connection

def _get_max_id(schema: str, table_name: str, primary_key: str, connection: Connection) -> int:
    """Get the maximum value of a primary key in a table.

   Args:
       schema (str): The schema of the table.
       table_name (str): The name of the table.
       primary_key (str): The name of the primary key.
       connection (Connection): The SQLAlchemy connection object.

   Returns:
       int: The maximum value of the primary key.
    """
    max_id = connection.exec_driver_sql(f'SELECT MAX({primary_key}) FROM {schema}.{table_name}').fetchone()[0]
    
    if max_id == None:
        max_id = 0
    return max_id

def _create_temp_table(schema: str, table_name: str, connection: Connection) -> None:
    """
   Create a temporary table with the same structure as the given table.

   Args:
       schema (str): The schema of the table.
       table_name (str): The name of the table.
       connection (Connection): The SQLAlchemy connection object.

   Returns:
       str: The name of the temporary table.
    """
    connection.exec_driver_sql(f'CREATE TEMP TABLE tmp_{table_name} AS SELECT * FROM {schema}.{table_name} LIMIT 0')
    return f'tmp_{table_name}'

def _verify_if_column_exists(schema: str, table_name: str, column_name: str, connection: Connection) -> bool:
    """Verify if a column exists in a table.

    Args:
        schema (str): The schema of the table.
        table_name (str): The name of the table.
        column_name (str): The name of the column.
        connection (Connection): The SQLAlchemy connection object.

    Returns:
        bool: True if the column exists, False otherwise.
    """
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata, autoload_with=connection)
    
    key = column_name.strip().lower()
    columns = [column.name.strip().lower() for column in table.columns]
    
    return key in columns

def get_connection() -> Connection:
    """  
    Establish a connection to the database.

    Returns:
        Connection: The SQLAlchemy connection object.
    """
    engine = create_engine(getenv('SQLALCHEMY_POSTGRES_CONN'))
    connection = engine.connect()
    
    return connection

def insert_data(df: DataFrame, schema: str, table_name: str, primary_key: str, connection: Connection, return_method: Literal['delta', 'full'] = 'delta') -> DataFrame:
    """
    Insert data into a table.

    Args:
        df (DataFrame): The data to be inserted.
        schema (str): The schema of the table.
        table_name (str): The name of the table.
        primary_key (str): The name of the primary key.
        connection (Connection): The SQLAlchemy connection object.
        return_method (Literal['delta', 'full'], optional): The method to return the data. Defaults to 'delta'.

    Returns:
        DataFrame: The inserted data.
    """
    max_id = _get_max_id(schema, table_name, primary_key, connection)
    
    df.to_sql(table_name, connection, if_exists='append', index=False, schema=schema)
    
    if return_method == 'delta':
        return read_sql_query(f'SELECT * FROM {schema}.{table_name} WHERE {primary_key} > {max_id}', connection)
        
    else: 
        return read_sql_query(f'SELECT * FROM {schema}.{table_name}', connection)    
    
def upsert_data(df: DataFrame, schema: str, table_name: str, primary_key: str, connection: Connection, keys: tuple[str], return_method: Literal['delta', 'full'] = 'delta') -> DataFrame:
    """
    Upsert data into a table.

    Args:
        df (DataFrame): The data to be upserted.
        schema (str): The schema of the table.
        table_name (str): The name of the table.
        primary_key (str): The name of the primary key.
        connection (Connection): The SQLAlchemy connection object.
        keys (tuple[str]): The keys to identify conflicts.
        return_method (Literal['delta', 'full'], optional): The method to return the data. Defaults to 'delta'.

    Returns:
        DataFrame: The upserted data.
    """
    
    max_id = _get_max_id(schema, table_name, primary_key, connection)
    
    tmp_table_name = _create_temp_table(schema, table_name, connection)
    
    df.to_sql(tmp_table_name, connection, if_exists='append', index=False)
    
    columns = df.columns.to_list()
    
    update_column_exists = _verify_if_column_exists(schema, table_name, 'data_alteracao_registro', connection)
    
    merge_sql = [
        f'INSERT INTO {schema}.{table_name} ({", ".join(columns)})', ' ',
        f'SELECT {", ".join(columns)} FROM {tmp_table_name}', ' ',
        f'ON CONFLICT ({", ".join(keys)}) DO UPDATE SET', ' ',
        f', '.join([f'{column} = EXCLUDED.{column}' for column in columns]), ' ',
        f', data_alteracao_registro = CURRENT_TIMESTAMP' if update_column_exists else '', ' ',
        
    ]
    merge_sql = ''.join(merge_sql)
    
    connection.exec_driver_sql(merge_sql)
    
    connection.exec_driver_sql(f'DROP TABLE {tmp_table_name}')
    
    if return_method == 'delta':
        return read_sql_query(f'SELECT * FROM {schema}.{table_name} WHERE {primary_key} > {max_id}', connection)
    
    else:
        return read_sql_query(f'SELECT * FROM {schema}.{table_name}', connection)

