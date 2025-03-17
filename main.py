from staging.staging_pipeline import staging_pipeline
from warehouse.warehouse_pipeline import warehouse_pipeline

try:
    print("============ Start Staging Pipeline ===========================")
    staging_pipeline()
    print("============ Success to run Staging Pipeline ===================")
    print("============ Start Warehouse Pipeline ==========================")
    warehouse_pipeline()
    print("============ Success to run warehouse_pipeline =================")
except:
    print("Error to run data pipeline!")  # Print the error message