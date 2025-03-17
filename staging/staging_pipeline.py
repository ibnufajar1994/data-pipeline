
# Get Key category
import os
from dotenv import load_dotenv
load_dotenv(".env")

key_file = os.getenv("KEY_CATEGORY")

#Import relevant module
from staging.extract_database import extract_database
from staging.extract_spreadsheet import extract_spreadsheet
from staging.load_staging import load_staging

def staging_pipeline():
    #Extract Database into pandas dataframe
    df_customers = extract_database("customers")
    df_employees = extract_database("employees")
    df_inventory_tracking = extract_database("inventory_tracking")
    df_order_details = extract_database("order_details")
    df_orders = extract_database("orders")
    df_products = extract_database("products")

    #Extract spreadsheet into pandas dataframe
    df_store_branch = extract_spreadsheet("store_branch", key_file)


    # Load data into staging table
    load_customers = load_staging(data=df_customers.iloc[:, :-1], schema="public",
                                table_name = "customers", idx_name = "customer_id",
                                source = "database"
                                )

    # Load data into employees
    load_employees = load_staging(data=df_employees.iloc[:, :-1], schema="public",
                                table_name = "employees", idx_name = "employee_id",
                                source = "database"
                                )
    # Load data into inventory_tracking
    load_inventory_tracking = load_staging(data=df_inventory_tracking.iloc[:, :-1], schema="public",
                              table_name = "inventory_tracking", idx_name = "tracking_id",
                              source = "database"
                              )
    # Load data into order_details
    load_order_details = load_staging(data=df_order_details.iloc[:, :-1], schema="public",
                              table_name = "order_details", idx_name = "order_detail_id",
                              source = "database"
                              )
    # Load data into orders
    load_orders = load_staging(data=df_orders.iloc[:, :-1], schema="public",
                              table_name = "orders", idx_name = "order_id",
                              source = "database"
                              )
    # Load data into products
    load_products = load_staging(data=df_products.iloc[:, :-1], schema="public",
                              table_name = "products", idx_name = "product_id",
                              source = "database"
                              )
    # Load data into store_branch
    load_store_branch = load_staging(data=df_store_branch.iloc[:, :-1], schema="public",
                              table_name = "store_branch", idx_name = "store_id",
                              source = "spreadsheet"
                              )
    # Run the function
    load_customers
    load_employees
    load_products
    load_orders
    load_order_details
    load_inventory_tracking
    load_store_branch