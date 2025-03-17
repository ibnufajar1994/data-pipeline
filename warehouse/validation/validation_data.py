import pandas as pd
from datetime import datetime
from helper. etl_log import etl_log
from helper.log_to_csv import log_to_csv


def validation_data(data: pd.DataFrame, table_name: str, validation_functions: dict) -> pd.DataFrame:
    """
    This function is used to validate data based on the specified validation functions.
    """
    try:
        # Create a report DataFrame
        report_data = {f'validate_{name}': data[name].apply(func) for name, func in validation_functions.items()}
        report_df = pd.DataFrame(report_data)

        # Summarize status data by all conditions
        report_df['all_valid'] = report_df.all(axis=1)

        # Filter out valid rows (all_valid = 'True')
        valid_data_df = data[report_df['all_valid']]

        # Filter out invalid rows (all_valid = 'False')
        invalid_data_df = data[~report_df['all_valid']]

        # Create success log message
        log_msg = {
            "step": "warehouse",
            "process": "validation",
            "status": "success",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
        return valid_data_df, invalid_data_df
    except Exception as e:
        # Create fail log message
        log_msg = {
            "step": "warehouse",
            "process": "validation",
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)
        log_to_csv(log_msg, "log.csv")
