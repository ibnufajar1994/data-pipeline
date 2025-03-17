from helper.db_conn import db_connection
import pandas as pd
def etl_log(log_msg: dict):
    """
    This function is used to save the log message to the database.
    """
    try:
        # create connection to database
        _, _, _, log_engine = db_connection()

        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        #extract data log
        df_log.to_sql(name = "etl_log",  # Your log table
                        con = log_engine,
                        if_exists = "append",
                        index = False)
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))

