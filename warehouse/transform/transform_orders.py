
import pandas as pd

from helper.etl_log import etl_log
from helper.log_to_csv import log_to_csv
from helper.handle_error_data import handle_error
from datetime import datetime
from warehouse.transform.clean_numeric_value import clean_numeric_column
from warehouse.extract.extract_staging import extract_staging
from warehouse.extract.extract_target import extract_target


def transform_orders(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform inventory_traking from staging database to be loaded on fct_tracking table on data warehouse.
    """
    try:
        process = "transformation"

#------------------------------------  start extract relevant data-----------------------------------------------------------#
        # Get dim_customers data
        dim_customers = extract_target("dim_customers")

        # Extract dim_employees table
        dim_employees = extract_target("dim_employees")


        #get dim_date data
        dim_date = extract_target("dim_date")

        #Extract data from staging order_details
        df_order_det = extract_staging("order_details", schema_name="public")

        # Extract dim_products data
        dim_products = extract_target("dim_products")

#------------------------------------  end extract relevant data-----------------------------------------------------------#

#------------------------------------Start Minor Cleaning ---------------------------------------------------------------#
        # drop column created_at
        data = data.drop(columns=['created_at'])

        # deduplication based on order_id
        data = data.drop_duplicates(subset='order_id')

        # Clean data to change timestamp into datetime only
        data["order_date"] = data["order_date"].dt.date

#------------------------------------End Minor Cleaning ---------------------------------------------------------------#

#----------------------------- start Transformation regarding customers data-----------------------------------------------------#
        #handle error data for customer_id
        error_data = data[data["customer_id"].isna()]

        #handle data that customer_id is missing then save it into MinIO
        if not error_data.empty:
            handle_error(data=error_data, bucket_name="error-paccafe", table_name="fct_order", process=process)


        # Drop missing data for customer_id
        data.dropna(subset=["customer_id"], inplace=True)

        #Cast customer_id into integer in order merge data with dim_customers on next process
        data["customer_id"] = data["customer_id"].astype("Int64")

        #Get sk_customer_id by mapping customer_id with nk_customer_id
        mapping = dim_customers.set_index("nk_customer_id")["sk_customer_id"]
        data["sk_customer_id"] = data["customer_id"].map(mapping)

        error_data = data[data["sk_customer_id"].isna()]

        #handle data that sk_customer_id is missing then save it into MinIO
        if not error_data.empty:
            handle_error(data=error_data, bucket_name="error-paccafe", table_name="fct_order", process=process)


        # Drop missing data for sk_customer_id
        data.dropna(subset="sk_customer_id", inplace=True)

        #Drop column customer_id
        data = data.drop(columns=["customer_id"])
#-----------------------------end transformation regarding customers data-----------------------------------------------------#



#-----------------------------start Transformation regarding employees data-----------------------------------------------------#

        #Get sk_employee_id from dim_employees by mapping between nk_employee_id and employee_id
        mapping = dim_employees.set_index("nk_employee_id")["sk_employee_id"]
        data["sk_employee_id"] = data["employee_id"].map(mapping)

        data = data.drop(columns=["employee_id"])
#-----------------------------End Transformation regarding employees data-----------------------------------------------------#


#-----------------------------Start transformation regarding  dim_date-----------------------------------------------------#
        #Get date_id from dim_date by matching change_date on inventory_tracking with date_actual on date_dim
        mapping = dim_date.set_index("date_actual")["date_id"]
        data["order_date"] = data["order_date"].map(mapping)

#-----------------------------End transformation regarding  dim_date-----------------------------------------------------#


#----------------------------------Start transformation regarding order_details table------------------------------------#

        #Get relevant data by join the dataframe between orders & order_details table
        data = data.merge(df_order_det[["order_id","product_id", "quantity", "unit_price","subtotal" ]],
                               on="order_id",
                               how="left"
                               )


#----------------------------------End transformation regarding order_details table------------------------------------#


#----------------------------------Start transformation regarding dim_products table------------------------------------#

        # Look Up sk_product_id base on nk_product_id and product_id
        data = data.merge( dim_products[["nk_product_id", "sk_product_id"]], 
            left_on="product_id", 
            right_on="nk_product_id", 
            how="left")

        # Delete Unnecessary Column
        data = data.drop(columns=["product_id", "nk_product_id"])
        data.dropna(subset="sk_product_id", inplace=True)

#----------------------------------End transformation regarding dim_products table------------------------------------#


        # rename column product_id to nk_order_id
        data = data.rename(columns={'order_id':'nk_order_id'})

        # deduplication based on nk_order_id
        data = data.drop_duplicates(subset='nk_order_id')




#-------------------------- Save log from if pipeline is succes to running -------------------------------------------#
 


        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": "fct_orders",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        return data
    
#-------------------------- Save log from if pipeline is failed to running -------------------------------------------#
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": "fct_orders",
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