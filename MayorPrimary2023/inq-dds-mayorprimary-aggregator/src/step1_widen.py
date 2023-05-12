import pandas as pd
import numpy as np
from src.utils import getSnapshotTime

def step1_widen(df,cand_rename,office_name):
    
    # exclude non-precincts
    dff = df[df['Precinct'].notna()]
    dff = dff[~dff['Precinct'].str.contains("CONGRESSIONAL")]

    # replace values in CandidateName field
    dff['CandidateName'] = dff['CandidateName'].str.strip()
    dff['CandidateName'] = np.where(dff['CandidateName'].isin(cand_rename.keys()),dff['CandidateName'],'Other')
    dff['CandidateName'] = dff.CandidateName.map(cand_rename)

    # filter to race we want and aggregate (widen) by precinct
    dff = dff[dff['RaceName']==office_name]\
       .pivot_table(index='Precinct',columns='CandidateName',values='CandidateVotes',aggfunc='sum').reset_index()

    # reformat precincts and get ward
    dff['prec_20'] = dff['Precinct'].str.replace("-","").str.strip().astype(int)
    dff['ward'] = dff['Precinct'].apply(lambda x: x.split("-")[0]).str.strip().astype(int)

    # aggregate by ward too
    dff_ward = dff.groupby('ward')[list(cand_rename.values())].sum().reset_index()
    
    print(f"Widened dataframes generated at {getSnapshotTime()}")
    
    # return two dataframes for use here
    return dff, dff_ward