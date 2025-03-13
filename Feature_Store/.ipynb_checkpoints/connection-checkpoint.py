import snowflake.connector
import pandas as pd
import os
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeConnection:
    def __init__(self):
        """Initialize Snowflake connection parameters from environment variables."""
        self.config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "QMRPQPQ-SD37645"),
            "user": os.getenv("SNOWFLAKE_USER", "2023DA04611"),
            "password": os.getenv("SNOWFLAKE_PASSWORD", "Wilp.bits-pilani.ac.in2023"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "CUSTOMER_CHURN"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        }
        self.conn = None

    def connect(self):
        """Establish a connection to Snowflake."""
        if not self.conn:
            self.conn = snowflake.connector.connect(**self.config)
        return self.conn

    def close(self):
        """Close the Snowflake connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query):
        """Execute a SELECT query and return a Pandas DataFrame."""
        conn = self.connect()
        cursor = conn.cursor()  #  Corrected: Use conn.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return pd.DataFrame(result, columns=columns)
        finally:
            cursor.close()

    def execute_non_select(self, query):
        """Execute INSERT, UPDATE, DELETE queries."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        finally:
            cursor.close()

    def commit(self):
        """Commit transactions."""
        self.conn.commit()

    def write_to_snowflake(self, df, table_name, overwrite=True):
        """
        Write a Pandas DataFrame to Snowflake.
        - Creates the table dynamically if it doesn't exist.
        - Overwrites or appends data based on `overwrite` flag.
        """
        conn = self.connect()
        cursor = conn.cursor()

        # Dynamically generate CREATE TABLE statement from DataFrame
        columns = ', '.join([f'"{col}" {self.get_snowflake_dtype(df[col])}' for col in df.columns])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        """
        cursor.execute(create_table_query)
        print(f" Table '{table_name}' created or already exists.")

        # Write DataFrame to Snowflake
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name, overwrite=overwrite)

        if success:
            print(f" Successfully inserted {nrows} rows into {table_name}")
        else:
            print(f" Data insertion failed for {table_name}.")

        cursor.close()

    def get_snowflake_dtype(self, series):
        """Infer Snowflake data types from Pandas series."""
        if pd.api.types.is_integer_dtype(series):
            return 'NUMBER'
        elif pd.api.types.is_float_dtype(series):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(series):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(series):
            return 'TIMESTAMP'
        else:
            return 'VARCHAR(255)'