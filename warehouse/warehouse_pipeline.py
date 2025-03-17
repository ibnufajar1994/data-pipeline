
#Import relevant module
import pandas as pd
from helper.handle_error_data import handle_error
from warehouse.extract.extract_staging import extract_staging

from warehouse.transform.transform_customers import transform_customers
from warehouse.transform.transform_employees import transform_employees
from warehouse.transform.transform_store_branch import transform_store_branch
from warehouse.transform.transform_products import transform_products
from warehouse.transform.transform_inventory import transform_inventory
from warehouse.transform.transform_orders import transform_orders


from warehouse.validation.validate_email_format import validate_email_format
from warehouse.validation.validate_phone_format import validate_phone_format
from warehouse.validation.validate_negative_value import validate_negatif_value
from warehouse.validation.validation_data import validation_data
from warehouse.validation.validate_role_format import validate_role_format
from warehouse.validation.validate_date_format  import validate_date_format
from warehouse.validation.validate_reason import validate_reason
from warehouse.validation.validate_payment import validate_payment
from warehouse.validation.validate_order_status import validate_order_status

from warehouse.load.load_to_warehouse import load_warehouse

def warehouse_pipeline():


    df_customers = extract_staging("customers", schema_name="public")   # Extract customers data from staging
    df_customers_tf  = transform_customers(df_customers, table_name="customers") # Transform customers data

    # Data validation for customers table
    valid_cust, invalid_cust = validation_data(data=df_customers_tf, table_name="customers", validation_functions={
        "email": validate_email_format,
        "phone": validate_phone_format,
        "loyalty_points": validate_negatif_value
    })

    #Load customers data to warehouse into dim_customers table
    load_warehouse(data=valid_cust, schema="public", table_name="dim_customers", idx_name="nk_customer_id", source="staging")

    #handle error data from customers table and load it to MinIO
    if (not invalid_cust.empty):
        handle_error(data=invalid_cust, bucket_name="error-paccafe", table_name="customers")

###-------------------------------NEXT PROCESS-------------------------------###

    df_employees = extract_staging("employees", schema_name="public") # Extract employees data from staging
    df_employees_tf = transform_employees(df_employees, table_name="employees") # Transform employees data

    # Data validation for employees table
    valid_emp, invalid_emp = validation_data(data=df_employees_tf, table_name="employees", validation_functions={
        "email": validate_email_format,
        "hire_date": validate_date_format,
        "role": validate_role_format
    })

    # Load employees data to warehouse into dim_employees table
    load_warehouse(data=valid_emp, schema="public", table_name="dim_employees", idx_name="nk_employee_id", source="staging")

    # Handle error data from employees table and load it to MinIO
    if (not invalid_emp.empty):
        handle_error(data=invalid_cust, bucket_name="error-paccafe", table_name="employees")


###-------------------------------NEXT PROCESS-------------------------------###

    df_store_branch = extract_staging("store_branch", schema_name="public") # Extract store_branch data from staging
    df_store_branch_tf = transform_store_branch(df_store_branch, table_name="store_branch")

    load_warehouse(data=df_store_branch_tf, schema="public", table_name="dim_store_branch", idx_name="nk_store_id", source="staging")

###-------------------------------------------------------NEXT PROCESS------------------------------------------------------------------###


    df_products = extract_staging("products", schema_name="public") # Extract products data from staging
    df_products_tf = transform_products(data=df_products, table_name="products") # Transform products data

    # Data validation for products table
    valid_prod, invalid_prod = validation_data(data=df_products_tf, table_name="products", validation_functions={
        "unit_price": validate_negatif_value,
        "cost_price": validate_negatif_value
    })

    # Load products data to warehouse into dim_products table
    load_warehouse(data=valid_prod, schema="public", table_name="dim_products", idx_name="nk_product_id", source="staging")

    # Handle error data from products table and load it to MinIO
    if (not invalid_prod.empty):
        handle_error(data=invalid_prod, bucket_name="error-paccafe", table_name="products")

###-------------------------------------------------------NEXT PROCESS------------------------------------------------------------------###

    df_inventory = extract_staging("inventory_tracking", schema_name="public") # Extract inventory data from staging
    df_inventory_tf = transform_inventory(data=df_inventory, table_name="inventory_tracking") # Transform inventory data

    # Data validation for inventory_tracking table
    valid_inv, invalid_inv = validation_data(data=df_inventory_tf, table_name="fct_inventory", validation_functions={
        "quantity_change": validate_negatif_value,
        "reason" : validate_reason
    })

    # Load inventory data to warehouse into fact_inventory table
    load_warehouse(data=valid_inv, schema="public", table_name="fct_inventory", idx_name = "nk_tracking_id", source="staging")

    # Handle error data from inventory_tracking table and load it to MinIO
    if (not invalid_inv.empty):
       handle_error(data=invalid_inv, bucket_name="error-paccafe", table_name="fct_inventory")

###-------------------------------------------------------NEXT PROCESS------------------------------------------------------------------###

    df_orders = extract_staging("orders", schema_name="public") # Extract orders data from staging
    df_orders_tf = transform_orders(data=df_orders, table_name="orders") # Transform orders data

    valid_ord, invalid_ord = validation_data(data=df_orders_tf, table_name="fct_orders", validation_functions={
        "quantity": validate_negatif_value,
        "unit_price": validate_negatif_value,
        "total_amount": validate_negatif_value,
        "payment_method": validate_payment,
        "order_status": validate_order_status

    })

    load_warehouse(data=valid_ord, schema="public", table_name="fct_order", idx_name="nk_order_id", source="staging")

    if (not invalid_ord.empty):
        handle_error(data=invalid_ord, bucket_name="error-paccafe", table_name="fct_order")




