from helper.auth_gspread import auth_gspread

def init_key_file(key_file:str):
    #define credentials to open the file
    gc = auth_gspread()

    #open spreadsheet file by key
    sheet_result = gc.open_by_key(key_file)

    return sheet_result