import pandas as pd
from helper.init_key_file import init_key_file

def extract_sheet(key_file:str, worksheet_name: str) -> pd.DataFrame:
    # init sheet
    sheet_result = init_key_file(key_file)

    # Access the specified worksheet within the spreadsheet
    worksheet_result = sheet_result.worksheet(worksheet_name)

    # Retrieve all values from the worksheet and create a DataFrame
    df_result = pd.DataFrame(worksheet_result.get_all_values())

    # set first rows as columns
    df_result.columns = df_result.iloc[0]

    # get all the rest of the values
    df_result = df_result[1:].copy()

    return df_result