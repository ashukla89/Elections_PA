import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

import requests
import json
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import time
import os


base = 'https://electionreturns.pa.gov'

supreme = '/General/CountyBreakDownResults?officeId=8&districtId=1&ElectionID=undefined&ElectionType=undefined&IsActive=undefined'
superior = '/General/CountyBreakDownResults?officeId=9&districtId=1&ElectionID=undefined&ElectionType=undefined&IsActive=undefined'
comm = '/General/CountyBreakDownResults?officeId=10&districtId=1&ElectionID=undefined&ElectionType=undefined&IsActive=undefined'

pages = {
    'supreme':supreme,
    'superior':superior,
    'comm':comm
}

chrome_options = Options()
chrome_options.add_argument("--headless")

driver = webdriver.Chrome(options=chrome_options)

docs = []

for key, value in pages.items():
    driver.get(base+value)
    time.sleep(3)
    html = driver.page_source
    # get timestamp
    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    doc = BeautifulSoup(html)

    # write html to the file with the time
    # define path
    file_path = 'HTMLs/page_'+key+'-'+ts+'.html'
    html = doc.prettify()
    # open and save to PDFs folder with the filename we saved earlier
    with open(file_path, 'w') as f:
        f.write(html)
        
    # find just the table we want
    table = doc.find('div',class_='container').find('div',{'ng-controller':'CountyBreakDownGeneralController'})\
        .find('div',class_='panel-body').find('div',class_='col-xs-9').find('div',class_='panel-body')\
        .find_all('div',{'ng-repeat':'districts in filterRecords(levels)'})

    data_standard = []
    data_retention = []

    for elem in table:
        # get county value from header
        county = elem.find('h5').text.strip()
        
        if "Retention" in county:
            county = county.split(" ")[0]
            # each of the 'item in district' divs represents a candidate
            cands = elem.find_all('div',{'ng-repeat':'item in districts'})
            for cand in cands:
                # one row of the final df for each candidate for each county
                row = {}
                row["County"] = county
                # each candidate contains two 'rows' of data
                r = cand.find_all('div',class_='row')
                # the first 'row' [0] contains candidate name, party, vote share, and raw votes
                row["Candidate"] = r[0].find(class_='col-sm-4').find_all('span')[0].text.strip()
                # no party data worth sharing, so skip that, as compared to standard partisan elections
                # separate Yes and No shared
                shares = r[0].find(class_='col-sm-5').find_all('div',class_='progress-bar')
                row["Yes Share"] = shares[0].text.strip()
                row["No Share"] = shares[1].text.strip()
                # split up the Votes section by colon marks/ spaces and parse accordingly
                # note that we have to split remove a pesky unicode character
                row["Yes Votes"] = r[0].find(class_='pull-left').text.split(":")[2].split("\xa0")[1].strip()
                row["No Votes"] = r[0].find(class_='pull-left').text.split(":")[3].strip()
                # the second 'row' [1] contains vote breakdown by method
    #             methods = r[1].find_all('div',class_='col-sm-4')
    #             row["Election Day"] = methods[0].text.split(":")[-1].strip()
    #             row["Mail"] = methods[1].text.split(":")[-1].strip()
    #             row["Provisional"] = methods[2].text.split(":")[-1].strip()
                # append that row to the overall data
                data_retention.append(row)
        else:
            # each of the 'item in district' divs represents a candidate
            cands = elem.find_all('div',{'ng-repeat':'item in districts'})
            for cand in cands:
                # one row of the final df for each candidate for each county
                row = {}
                row["County"] = county
                # each candidate contains two 'rows' of data
                r = cand.find_all('div',class_='row')
                # the first 'row' [0] contains candidate name, party, vote share, and raw votes
                details = r[0].find(class_='col-sm-4').find_all('span')
                row["Candidate"] = details[0].text.strip()
                row["Party"] = details[1].text.strip().replace("(","").replace(")","")
                row["Share"] = r[0].find(class_='col-sm-5').text.strip()
                row["Votes"] = r[0].find(class_='col-sm-3').text.split(":")[-1].strip()
                # the second 'row' [1] contains vote breakdown by method
    #             methods = r[1].find_all('div',class_='col-sm-4')
    #             row["Election Day"] = methods[0].text.split(":")[-1].strip()
    #             row["Mail"] = methods[1].text.split(":")[-1].strip()
    #             row["Provisional"] = methods[2].text.split(":")[-1].strip()
                # append that row to the overall data
                data_standard.append(row)

    # turn into df
    df_s = pd.DataFrame(data_standard)
    df_r = pd.DataFrame(data_retention)

    # save as csvs, provided the df_r one actually exists 
    df_s.to_csv('Data/data_'+key+'_partisan_'+ts+'.csv',index=False)
    if len(df_r) > 0:
        df_r.to_csv('Data/data_'+key+'_retention_'+ts+'.csv',index=False)