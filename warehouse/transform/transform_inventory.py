
import pandas as pd

from helper.etl_log import etl_log
from helper.log_to_csv import log_to_csv
from helper.handle_error_data import handle_error
from datetime import datetime
from warehouse.transform.clean_numeric_value import clean_numeric_column
from warehouse.extract.extract_target import extract_target


def transform_inventory(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform inventory_traking from staging database to be loaded on fct_tracking table on data warehouse.
    """
    try:
        process = "transformation"
        # rename column product_id to nk_tracking_id
        data = data.rename(columns={'tracking_id':'nk_tracking_id'})

        # deduplication based on nk_tracking_id
        data = data.drop_duplicates(subset='nk_tracking_id')

        # drop column created_at
        data = data.drop(columns=['created_at'])

        # Clean data to change timestamp into datetime only
        data['change_date'] = data['change_date'].dt.date

        #Extract Target from dim_products
        df_dim_products = extract_target("dim_products")

        #Create Mapping index between nk_product_id dan sk_product_id on df_dim_products
        mapping = df_dim_products.set_index("nk_product_id")["sk_product_id"]

        # Get sk_product_id on dim_product by matching nk_product_id on dim_product with product_id on inventory_tracking
        data["sk_product_id"] = data["product_id"].map(mapping)

        #put error_data where sk_product_id is none after mapping
        error_data = data[data["sk_product_id"].isna()]
        #drop error_data from data
        data = data.dropna(subset=["sk_product_id"])

        #Drop column product_id
        data = data.drop(columns=["product_id"])



        #get dim_date data
        dim_date = extract_target("dim_date")

        #Get date_id from dim_date by matching change_date on inventory_tracking with date_actual on date_dim
        mapping_date = dim_date.set_index("date_actual")["date_id"]
        data["change_date"] = data["change_date"].map(mapping_date)



        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": "inventory_tracking",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        #handle data that don't have sk_product_id then save it into MinIO
        if not error_data.empty:
            handle_error(data=error_data, bucket_name="error-paccafe", table_name="fct_inventory", process=process)

        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": "inventory_tracking",
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