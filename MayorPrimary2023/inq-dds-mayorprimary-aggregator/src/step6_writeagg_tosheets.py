import json
import requests
import os
import glob
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv('.env')
import pandas as pd
import numpy as np

import requests
import json
import boto3

import string

# set up to write to Google sheets
import gspread
#import gspread_pandas as gspd
from gspread_pandas import Spread, Client, conf

from src.utils import getSnapshotTime

def step6_writeagg_tosheets(sh,agg_dict,pct_agg_dict):
    
    sh.clear_sheet(sheet='Aggregations')

    abs_test_dict = agg_dict.copy()
    abs_test_out = {}

    for key, test in abs_test_dict.items():
        #test = value.copy()
        test.loc[-1] = test.columns
        test.index = test.index + 1
        test.columns = ['Measure'] + list(test.columns[1:])
        test = test.sort_index()
        abs_test_out[key] = test

    pct_test_dict = pct_agg_dict.copy()
    pct_test_out = {}

    for key, test in pct_test_dict.items():
        #test = value.copy()
        test.loc[-1] = test.columns
        test.index = test.index + 1
        test.columns = ['Measure'] + list(test.columns[1:])
        test = test.sort_index()
        pct_test_out[key] = test

    blank_row = pd.DataFrame([""] * len(test.columns)).T
    blank_row.columns = test.columns

    abs_long = pd.concat([pd.concat([t,blank_row],axis=0,ignore_index=True)\
              for t in list(abs_test_out.values())],axis=0,ignore_index=True)

    pct_long = pd.concat([pd.concat([t,blank_row],axis=0,ignore_index=True)\
              for t in list(pct_test_out.values())],axis=0,ignore_index=True)

    blank_col = pd.DataFrame([""] * len(abs_long))

    all_agg = pd.concat([abs_long,blank_col,pct_long],axis=1)
    all_agg.columns = all_agg.iloc[0]
    all_agg = all_agg.drop(0)

    sh.df_to_sheet(all_agg,index=False,sheet="Aggregations",replace=False)

    print(f"All aggregations written to Google Sheets at {getSnapshotTime()}")