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

from datawrapper import Datawrapper

# from src.utils import postMessageToSlack, saveDataToS3, getDataFromS3, getSnapshotTime
from src.utils import getSnapshotTime

from src.step1_widen import step1_widen
from src.step2_writewide_tosheets import step2_writewide_tosheets
from src.step3_jointobins import step3_jointobins
from src.step4_aggregate import step4_aggregate
from src.step5_sendto_dw import step5_sendto_dw
from src.step6_writeagg_tosheets import step6_writeagg_tosheets

def wrapper():

    print("--------------------------")
    print(f"Begin wrapper script at {getSnapshotTime()}")
    
    ### INSTANTIATE GOOGLE SHEETS OBJECT FOR USE ###

    # Set the path to the JSON file, where the file has to be called 'google_secret.json'
    # GO UP A COUPLE IN THE TREE
    # KASTURI TO FIX
    json_path = os.path.join(os.getcwd()) 

    # Load the configuration from the JSON file
    c = conf.get_config(json_path)

    # Instantiate sheets
    sh = Spread('Mayor_primary_results_2023',config=c)
    
    print(f"Google sheet object instantiated at {getSnapshotTime()}")
    
    # read in human-editable sheet with list of aggregations to viz, which is allowed to have duplicate key values
    ref = sh.sheet_to_df(sheet='Keys_and_dwids').reset_index()
    
    # Load in reference tables of candidate names in test data (senate_g22) and actual (mayor_p23)
    cand_ref_old = sh.sheet_to_df(sheet='Cand_color_map_old').reset_index()
    cand_ref_new = sh.sheet_to_df(sheet='Cand_color_map_new').reset_index()
    
    print(f"Reference sheets read in at {getSnapshotTime()}")
    
    ### SPECIFY GLOBAL VARIABLES TO BE USED ###

    # the name of the office as it's showing up in the city's data
    # office_name = 'UNITED STATES SENATOR<br/>(VOTE FOR 1)'
    office_name = 'MAYOR DEM<br/>Democratic (VOTE FOR 1)'
    
    # DECIDE HERE which list of candidate mappings we're reading from
    # use_cand = cand_ref_old
    use_cand = cand_ref_new

    # Map names of candidates to display
    cand_rename = dict(use_cand[['Cand_name_raw','Cand_name_display']].values)
    # Map colors corresponding to candidate names
    cand_list = dict(use_cand[['Cand_name_display','Color']].values)

    print(f"Global variables defined at {getSnapshotTime()}")
    
    ### READ IN RESULTS AND DEMOGRAPHIC DATA FROM S3 ###
    
    df = pd.read_csv('https://inq-dds-election-data.s3.amazonaws.com/election-results/2023/live/primary/raw_tidy/results.csv')
    bins = pd.read_csv('https://inq-dds-election-data.s3.amazonaws.com/election-results/2023/live/primary/raw_tidy/mayor_bins.csv')
    #df = pd.read_csv('../Philadelphia County Results.csv',index_col=False)
    #bins = pd.read_csv('../mayor_bins.csv')

    print(f"Raw data read in at {getSnapshotTime()}")
    
    ### STEP 1: RUN SCRIPT TO MAKE DATA WIDE AND RETURN IT ###
    
    dff, dff_ward = step1_widen(df,cand_rename,office_name)

    ### STEP 2: WRITE WIDE DATA BY PREC AND WARD TO GOOGLE SHEETS ### DO THIS LAST TOO
    
    step2_writewide_tosheets(sh,dff,dff_ward)

    ### STEP 3: RUN SCRIPT TO APPEND WIDE DATA TO PRE-BINNED PRECINCT ATTRIBUTES ###
    
    dfb = step3_jointobins(dff,bins)

    ### NEED SYNTAX FROM KASTURI ###
    ### STORE AS '2023/live/primary/wide_with_demos/results_with_demos.csv'
    
    # saveDataToS3(dfb)

    ### STEP 4: GENERATE AGGREGATIONS AND RETURN IN TWO DICT OBJECTS ###
    
    agg_dict, pct_agg_dict, cands_ordered, citywide = step4_aggregate(ref,cand_list,dfb,sh)

    ### STEP 5: SEND AGGREGATIONS TO DATAWRAPPER AND RETURN UPDATED CHART REFERENCE ###
    
    new_ref = step5_sendto_dw(dfb,agg_dict,pct_agg_dict,citywide,cands_ordered,cand_list,ref)
    
    ### Update the reference sheet with our new additions
    sh.clear_sheet(sheet='Keys_and_dwids')
    sh.df_to_sheet(new_ref,index=False,sheet='Keys_and_dwids')
    
    ### STEP 6: WRITE AGGREGATIONS TO GOOGLE SHEETS
    
    step6_writeagg_tosheets(sh,agg_dict,pct_agg_dict)
    
    print(f"Process completed at {getSnapshotTime()}")