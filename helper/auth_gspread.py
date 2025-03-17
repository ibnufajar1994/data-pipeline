#import important module

from oauth2client.service_account import ServiceAccountCredentials
import gspread
from dotenv import load_dotenv
import os

#Load the env
load_dotenv(".env")

#Get the credentials
CRED_PATH = os.getenv("CRED_PATH")

# Define function to authorize the API from Gspread
def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    #Define api credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_PATH, scope) # Your json file here

    gc = gspread.authorize(credentials)

    return gc