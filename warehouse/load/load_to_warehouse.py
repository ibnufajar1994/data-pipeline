from helper.db_conn import db_connection
from datetime import datetime
from helper.etl_log import etl_log
from helper.log_to_csv import log_to_csv
from helper.handle_error_data import handle_error
from pangres import upsert

def load_warehouse(data, schema:str, table_name: str, idx_name:str, source:str):
    try:
        # create connection to database
        _, _, dwh_engine, _ = db_connection()

        # set data index or primary key
        data = data.set_index(idx_name)

        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = dwh_engine,
                df = data,
                table_name = table_name,
                schema = schema,
                if_row_exists = "update")

        #create success log message
        log_msg = {
                "step" : "warehouse",
                "process":"load",
                "status": "success",
                "source": source,
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        # return data
    except Exception as e:

        #create fail log message
        log_msg = {
            "step" : "warehouse",
            "process":"load",
            "status": "failed",
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }
        print(e)
        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error-paccafe', table_name= table_name, process='load')
        except Exception as e:
            print(e)
    finally:
        etl_log(log_msg)
        log_to_csv(log_msg, "log.csv")