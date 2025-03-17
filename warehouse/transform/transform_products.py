
import pandas as pd

from helper.etl_log import etl_log
from helper.log_to_csv import log_to_csv
from helper.handle_error_data import handle_error
from datetime import datetime
from warehouse.transform.clean_numeric_value import clean_numeric_column
from warehouse.extract.extract_target import extract_target

def transform_products(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform products from staging database to be loaded on dim_products table on data warehouse.
    """
    try:
        process = "transformation"
        # rename column product_id to nk_product_id
        data = data.rename(columns={'product_id':'nk_product_id'})

        # deduplication based on nk_product_id
        data = data.drop_duplicates(subset='nk_product_id')

        # drop column created_at
        data = data.drop(columns=['created_at'])

        #clean numeric value in column unit_price
        data['unit_price'] = clean_numeric_column(data['unit_price'])

        #clean numeric value in column cost_price
        data["cost_price"] = clean_numeric_column(data["cost_price"])

        #Extract Target from dim_store_branch
        df_dim_store_branch = extract_target("dim_store_branch")

        #Lookup `store_branch id` from `dim_store_branch` table based on `store_name`
        data['sk_store_branch'] = data["store_branch"].apply(lambda x: df_dim_store_branch.loc[df_dim_store_branch["store_name"] == x, "sk_store_id"].values[0])
        
        #drop unnecessary column
        data = data.drop(columns="store_branch")
 

    

        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": "products",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }

        return data
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": "products",
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