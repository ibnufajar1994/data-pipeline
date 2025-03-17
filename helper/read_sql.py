# Import relevant module
from dotenv import load_dotenv
load_dotenv(".env")
import os
MODEL_PATH = os.getenv("MODEL_PATH") #get path data of sql model

# Define the read_sql function to read sql file base on model_path and sql file name
def read_sql(table_name):
    
    file_path = os.path.join(MODEL_PATH, f"{table_name}.sql")

    # open your file .sql
    with open(file_path, 'r') as file:
        content = file.read()

    return content
