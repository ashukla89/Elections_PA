from dotenv import load_dotenv
load_dotenv('.env')
import pandas as pd
from src.utils import getSnapshotTime

# set up to write to Google sheets
import gspread
#import gspread_pandas as gspd
from gspread_pandas import Spread, Client, conf

def step2_writewide_tosheets(sh,dff,dff_ward):
    
    sh.clear_sheet(sheet='Prec_results_wide')
    sh.df_to_sheet(dff,index=False,sheet='Prec_results_wide')

    sh.clear_sheet(sheet='Ward_results_wide')
    sh.df_to_sheet(dff_ward,index=False,sheet='Ward_results_wide')
    
    print(f"wide data printed to Sheets at {getSnapshotTime()}")