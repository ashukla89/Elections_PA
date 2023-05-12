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

def step4_aggregate(ref,cand_list,dfb,sh):
    
    # pull out a list of keys (column names) from that, which CANNOT have duplicates
    agg_list = list(ref.Key.unique())

    # prepare data at the citywide level for saving, but more importantly for ordering candidates
    citywide = dfb[cand_list].sum().reset_index().rename(columns={"index":"candidate",0:"votes_abs"})\
        .sort_values(by='votes_abs',ascending=False)
    citywide['votes_pct'] = citywide['votes_abs'] / citywide.votes_abs.sum() * 100

    # get the list of candidates to display by share of citywide vote
    cands_ordered = citywide.sort_values(by='votes_abs',ascending=False).candidate.tolist()

    # specify the sort order by field
    sort_order = {
        'race_bucket_1':['Black >75%','Black 50-75%','White >75%','White 50-75%','Latino >75%','Latino 50-75%',
                         'AAPI 50-75%','No Majority'],
        'income_bucket':['<$50k','$50-75k','$75-100k','$100k+'],
        'inc_change_bucket':['got poorer','got richer'],
        'pov_bucket':['Less than 10% poverty','10-20% poverty','20-30% poverty','30-50% poverty',\
                     '50%+ poverty'],
        'pov_change_bucket':['less poverty','more poverty'],
        'pop_delta_bins':['<0% growth','0-5% growth','5-10% growth','10%+ growth'],
        'edu_bin_2':['20% advanced degree or higher',
                     '25% bachelor degree or higher',
                     '40% some college or higher',
                    'All other'],
        'parent_structure':['Fewer than 20% of households have children',
                            'Fewer than 50% of households have two parents',
                           'More than 50% of of households have two parents'],
        'school_type_bucket1':['Fewer than 20% of households have children',
                              'Fewer than 1/4 of children in private school',
                              'Between 1/4 and 1/3 of children in private school',
                              'More than 1/3 of children in private school'],
        'shootings_bucket1':['Fewer than 25','25-74','75-174','175-299','300+'],
        'cat_30under_C':["Fewer than 17% adults under 30",
                         "17-21% adults under 30",
                         "21% or more adults under 30"],
        'cat_newvoter':["Fewer than 25% new voters",
                        "25-33% new voters",
                        "33% or more new voters"],
        'cat_voted2021':["Fewer than 18% voted in 2021",
                         "18-24% voted in 2021",
                         "24-34% voted in 2021",
                         "34% or more voted in 2021"],
        'turn_cat':["17-50% voter turnover",
                    "50-58% voter turnover",
                    "58-67% voter turnover",
                    "67% or greater voter turnover"],
        'ballotreq_cat':["0-7% mail ballot requests this year",
                         "7-11% mail ballot requests this year",
                         "11-43% mail ballot requests this year"],
        'cat_vbm2020':["16-41% mail vote in 2020",
                       "41-49% mail vote in 2020",
                       "49-60% mail vote in 2020",
                       "60-95% mail vote in 2020"],
        'market_value_tier':["Median value less than $100k",
                            "Median value from $100k to $175k",
                            "Median value from $175k to $250k",
                            "Median value from $250k to $500k",
                            "Median value greater than $500k",
                            "No valued homes"],
        '%_over_median_tier':["Nearly all homes below city median value",
                             "Most homes below city median value",
                             "Most homes above city median value",
                             "Nearly all homes above city median value",
                             "No valued homes"],
        "%_change_assessed_value_tier":["Median value increased modestly or decreased",
                                       "Median value increased by over a fifth",
                                       "Median value increased by over two-fifths",
                                       "Median value increased by over three-fifths",
                                       "No valued homes"],
        "total_livable_area_tier":["Under 1000 sq ft",
                                  "1000-1200 sq ft",
                                  "1200-1400 sq ft",
                                  "Over 1400 sq ft",
                                  "No valued homes"]
    }

    # generate the aggregations
    agg_dict = {}
    pct_agg_dict = {}
    for col in agg_list:
        piv = dfb.pivot_table(index=col,values=cands_ordered,aggfunc='sum')[cands_ordered]
        # if it's cluster, reorder by raw total votes by category
        if col == 'clust_name':
            order_list = dfb.groupby(col)[cands_ordered].sum().sum(axis=1).sort_values(ascending=False).index.tolist()
            piv = piv.reindex(order_list)
        # otherwise, if it's in the sort_order dict above, sort it accordingly
        if col in sort_order.keys():
            piv = piv.reindex(sort_order[col])
        # calculate the same percentage-wise
        pct_piv = (piv.div(piv.sum(axis=1),axis=0) * 100).round(1)
        agg_dict[col] = piv.reset_index()
        pct_agg_dict[col] = pct_piv.reset_index()
        
    print(f"Aggregations generated at {getSnapshotTime()}")
    
    return agg_dict, pct_agg_dict, cands_ordered, citywide