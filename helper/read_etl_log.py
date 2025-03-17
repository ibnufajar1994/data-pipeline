from helper.db_conn import db_connection
from helper.read_sql import read_sql
import sqlalchemy
import pandas as pd
    
def read_etl_log(filter_params: dict):
    """
    function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """
    try:
        # create connection to database
        _, _, _, log_engine = db_connection()

        # To help with the incremental process, get the etl_date from the relevant process
        """
        SELECT MAX(etl_date)
        FROM etl_log "
        WHERE
            step = %s and
            table_name ilike %s and
            status = %s and
            process = %s
        """
        query = sqlalchemy.text(read_sql("log"))

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=log_engine, params=(filter_params))

        #return extracted data
        return df
    except Exception as e:
        print("Can't execute your query. Cause: ", str(e))