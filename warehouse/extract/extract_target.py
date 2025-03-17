import pandas as pd
from helper.db_conn import db_connection

def extract_target(table_name: str):
    """
    this function is used to extract data from the data warehouse.
    """
    _, _, dwh_engine, _,  = db_connection()

    # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
    query = f"SELECT * FROM {table_name}"

    # Execute the query with pd.read_sql
    df = pd.read_sql(sql=query, con=dwh_engine)

    return df