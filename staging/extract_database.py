from helper.db_conn import db_connection
import pandas as pd
from datetime import datetime
import sqlalchemy
from helper.etl_log import etl_log
from helper.read_sql import read_sql
from helper.read_etl_log import read_etl_log
from datetime import datetime
from helper.log_to_csv import log_to_csv


def extract_database(table_name: str):

    try:
        # create connection to database
        src_engine, _, _, _, = db_connection()

        # Get date from previous process
        filter_log = {"step_name": "staging",
                    "table_name": table_name,
                    "status": "success",
                    "process": "load"}
        etl_date = read_etl_log(filter_log)


        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if(etl_date['max'][0] == None):
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date['max'][0]

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        """
        SELECT *
        FROM customers
        WHERE created_at > :etl_date
        """
        query = sqlalchemy.text(read_sql(table_name))

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=src_engine, params=({"etl_date":etl_date},))
        log_msg = {
                "step" : "staging",
                "process":"extraction",
                "status": "success",
                "source": "database",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        log_msg = {
            "step" : "staging",
            "process":"extraction",
            "status": "failed",
            "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)
        log_to_csv(log_msg, "log.csv")