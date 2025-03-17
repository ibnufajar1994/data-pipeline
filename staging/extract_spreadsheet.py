from helper.extract_sheet import extract_sheet
from datetime import datetime
from helper.log_to_csv import log_to_csv
from dotenv import load_dotenv
import os
load_dotenv(".env")
key_file = os.getenv("KEY_CATEGORY")

def extract_spreadsheet(worksheet_name: str, key_file: str):

    try:
        # extract data
        df_data = extract_sheet(worksheet_name = worksheet_name,
                                    key_file = key_file)

        # success log message
        log_msg = {
            "step" : "staging",
            "status": "success",
            "process": "extraction",
            "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    except Exception as e:
        # fail log message
        log_msg = {
            "step" : "extraction",
            "status": "failed",
            "process": "extraction",
            "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    finally:
        # load log to csv file
        log_to_csv(log_msg, 'log.csv')

    return df_data
