import pandas as pd
from src.utils import getSnapshotTime

def step3_jointobins(dff,bins):

    # remove raw data columns previously used for calculating demographics at prec level
    dfb = dff.merge(bins[[col for col in bins.columns if "B" not in col]],on='prec_20')
    dfb.columns = [col.strip() for col in dfb.columns]

    # generate ward column
    dfb['Precinct'] = dfb['Precinct'].str.strip()
    dfb['ward'] = dfb['Precinct'].apply(lambda x: x.split("-")[0]).astype(int)

    # map cluster values to the way we describe them
    map_clust = {
        0:"Pro-establishment Black voters",
        1:"Poor and Latino voters",
        2:"Younger white progressive voters",
        3:"Working-class white moderate voters",
        4:"Wealthy white liberal voters",
        5:"Less politically affiliated Black voters"
    }
    dfb['clust_name'] = dfb['clust_6'].map(map_clust)
    
    print(f"wide data joined to demographic data at {getSnapshotTime()}")

    return dfb