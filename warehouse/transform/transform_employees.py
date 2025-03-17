
import pandas as pd

from helper.etl_log import etl_log
from helper.log_to_csv import log_to_csv
from helper.handle_error_data import handle_error
from datetime import datetime

def transform_employees(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform employees from staging database to be loaded on dim_employees table on data warehouse.
    """
    try:
        process = "transformation"
        # rename column employee_id to nk_employee_id
        data = data.rename(columns={'employee_id':'nk_employee_id'})

        # deduplication based on nk_employee_id
        data = data.drop_duplicates(subset='nk_employee_id')

        # drop column created_at
        data = data.drop(columns=['created_at'])

        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": "employees",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }

        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": "employees",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }

         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error-paccafe', table_name= table_name, process=process)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)
        log_to_csv(log_msg,"log.csv")