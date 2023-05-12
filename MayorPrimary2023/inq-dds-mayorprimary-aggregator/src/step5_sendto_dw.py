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

from datawrapper import Datawrapper

from src.utils import getSnapshotTime

def step5_sendto_dw(dfb,agg_dict,pct_agg_dict,citywide,cands_ordered,cand_list,ref):

    # instantiate DW object
    # dw = Datawrapper(access_token = env['DW_API_TOKEN'])
    DW_API_TOKEN = '39k6yPxWt3442Rz2IDWKoEGqS9cKBoPYJaQ6JIXawom8SOcNWq1g60LisoeSU7WF'
    dw = Datawrapper(access_token = DW_API_TOKEN)
    
    ### Update the DW charts at the top: citywide bar chart and stacked percentage charts ###
    
    # prepare timestamp for metadata updatingdfb,agg_dict,pct_agg_dict,citywide,cands_ordered
    time_var = datetime.now().strftime("%I:%M:%S %p, %b %d")
    summary_updates = {
        'visualize': {
                      'color-category':{'map':cand_list},
                        'categoryOrder': cands_ordered
        },
        'annotate' : {
            'notes': f'<i>Last update: {int(dfb[cands_ordered].sum().sum()):,} votes at {time_var}</i>'
        }
    }

    # update data, time, and color styles in abs chart
    dw.add_data('5HJwc',citywide)
    dw.update_metadata('5HJwc',summary_updates)

    # update data, time, and color styles in pct chart
    dw.add_data('49nLG',citywide[['candidate','votes_pct']].set_index('candidate').T.reset_index())
    dw.update_metadata('49nLG',summary_updates)

    # publish the charts
    dw.publish_chart('5HJwc')
    dw.publish_chart('49nLG')
    
    print(f"Citywide summary charts updated at {getSnapshotTime()}")
    
    ### Send aggregations to DataWrapper Maps ###
    
    ### Start with leading candidate ###
    
    # need a DF that groups candidates by winning margin

    # copy the dfs we want for modification and also get percentage columns
    leader_prec = dfb.set_index(['prec_20','Precinct'])[cands_ordered]
    # multiply by 100 and ensure the PrecinctName field is there
    leader_prec_pct = (leader_prec.div(leader_prec.sum(axis=1),axis=0) * 100).round(1)
    # and rename columns
    leader_prec_pct.columns = [col+"_pct" for col in leader_prec_pct.columns]
    # join them together to ensure you have both absolute and pct
    leader_prec = leader_prec.merge(leader_prec_pct,left_index=True,right_index=True).reset_index()

    leader_ward = agg_dict['ward'].copy().set_index('ward')
    leader_ward_pct = pct_agg_dict['ward'].set_index('ward')
    # rename pct columns
    leader_ward_pct.columns = [col+"_pct" for col in leader_ward_pct.columns]
    # join them together to ensure you have both absolute and pct
    leader_ward = leader_ward.merge(leader_ward_pct,left_index=True,right_index=True).reset_index()

    # calculate total votes
    leader_prec['total'] = leader_prec[cands_ordered].sum(axis=1)
    leader_ward['total'] = leader_ward[cands_ordered].sum(axis=1)

    # determine who is leading
    leader_prec['leader'] = leader_prec[cands_ordered].idxmax(axis=1)
    leader_ward['leader'] = leader_ward[cands_ordered].idxmax(axis=1)

    # get a proper ward and division number for the Precinct chart
    leader_prec['Ward'] = leader_prec['Precinct'].apply(lambda x: x.split("-")[0]).astype(int)
    leader_prec['Division'] = leader_prec['Precinct'].apply(lambda x: x.split("-")[1]).astype(int)

    map_properties = {
#         'visualize' : {
#             'colorscale': {'map': cand_list},
#             'max-height': 550,
#             'max-map-height': 550
#         },
        'annotate' : {
            'notes': f'<i>Last update: {int(dfb[cand_list.keys()].sum().sum()):,} votes at {time_var}</i>'
        }
#         ,
#         'describe' : {
#             'source-name':f'Inquirer analysis by ASEEM SHUKLA of Philadelphia City Commissioners data'
#         }
    }

    dw.add_data('hEco6',leader_prec)
    dw.add_data('BlT5Q',leader_ward)

    dw.update_metadata('hEco6',map_properties)
    dw.update_metadata('BlT5Q',map_properties)

    # publish the chart
    dw.publish_chart('hEco6')
    dw.publish_chart('BlT5Q')
    
    print(f"Horse race maps updated at {getSnapshotTime()}")
    
    ### Likewise for turnout ###
    ### ASEEM TO UPDATE THIS LOGIC TO REFLECT NEW WARD CALCULATIONS ###
    
    # votes by precinct
    turn_prec = pd.concat([dfb.set_index(['prec_20','Precinct'])[cands_ordered].sum(axis=1).reset_index().\
        rename(columns={0:'votes'}),dfb['reg_dems']],axis=1)

    # votes by ward
    turn_ward = pd.concat([dfb.groupby('ward')[cands_ordered].sum().sum(axis=1),\
               dfb.groupby('ward').reg_dems.sum()],axis=1).rename(columns={0:'votes'}).reset_index()

    # get a proper ward and division number for the Precinct chart
    turn_prec['Ward'] = turn_prec['Precinct'].apply(lambda x: x.split("-")[0]).astype(int)
    turn_prec['Division'] = turn_prec['Precinct'].apply(lambda x: x.split("-")[1]).astype(int)

    # calculate turnout
    turn_prec['turnout'] = turn_prec['votes'] / turn_prec['reg_dems'] * 100
    turn_ward['turnout'] = turn_ward['votes'] / turn_ward['reg_dems'] * 100

    # add data
    dw.add_data('mFoD0',turn_prec)
    dw.add_data('NUiRQ',turn_ward)

    # add timestamp
    map_properties = {
#         'visualize' : {
#             'max-height': 550,
#             'max-map-height': 550
#         },
        'annotate' : {
            'notes': f'<i>Last update: {int(dfb[cands_ordered].sum().sum()):,} votes at {time_var}</i>'
        }
#             ,
#         'describe' : {
#             'source-name':f'Inquirer analysis by LEO CASSEL-SISKIND of Philadelphia City Commissioners data'
#         }
    }

    # update timestamp
    dw.update_metadata('mFoD0',map_properties)
    dw.update_metadata('NUiRQ',map_properties)

    # publish the chart
    dw.publish_chart('mFoD0')
    dw.publish_chart('NUiRQ')
    
    print(f"Turnout maps updated at {getSnapshotTime()}")
    
#     ### Make sure our 'model' chart is updated ###
    
#     basic_properties = {
#         'visualize':{
#             'color-category':{'map':cand_list}
#         }
#     }

#     dw.update_metadata('2Qcax',basic_properties)
#     dw.update_metadata('t6OvN',basic_properties)

#     dw.publish_chart('2Qcax')
#     dw.publish_chart('t6OvN')

#     # save all those vis properties
#     model_abs_vis = dw.chart_properties('2Qcax')['metadata']['visualize']
#     model_pct_vis = dw.chart_properties('t6OvN')['metadata']['visualize']
    
#     ### And now loop through all the other bar charts ###
    
    # prepare timestamp for metadata updating
    time_update = {    'annotate' : {
            'notes': f'<i>Last update: {int(dfb[cands_ordered].sum().sum()):,} votes at {time_var}</i>'
        }
    }

    # Create a citywide top row to place at the top of the pct charts

    bottom = citywide[['candidate','votes_pct']].set_index('candidate').T.reset_index()
    bottom.iloc[0,0] = "Total"
    bottom['group'] = 'Citywide'

    # iterate through list of reference DW ids and update (or create) charts

    for i in range(len(ref)):
        print(ref.iloc[i]['Key'])


        ### UPDATE OR CREATE ABSOLUTE CHART ###

        # whatever we do, add a bottom row to the data
        # first get the dataframe
        abs_data = agg_dict[ref.iloc[i]['Key']].copy()
        # add a column in pct_data for grouping, so it shows up properly
        abs_data['group'] = '&nbsp;'

        # if chart ID exists, update just data
        if len(ref.iloc[i]['DW_id_abs'])==5:
            dw.add_data(ref.iloc[i]['DW_id_abs'],abs_data)
            print(f"Abs chart data updated at {getSnapshotTime()}")

        # otherwise create the chart
        else:
            sub_chart_abs = dw.create_chart(
                title = f"{ref.iloc[i]['Title']} (Total Votes)", 
                chart_type = 'd3-bars-stacked',
                data = abs_data,
                folder_id = '159754'
            )
            # update the value in the reference sheet
            ref.iloc[i]['DW_id_abs'] = sub_chart_abs['id']
            print(f"No abs chart id found, new chart created  at {getSnapshotTime()}, the ID is {ref.iloc[i]['DW_id_abs']}")

        # either way, update all the metadata for styling
#         model_props_abs = {
#             'visualize':model_abs_vis,
#             'describe':{'source-name':f'Inquirer analysis by {ref.iloc[i].Byline.upper()} of {ref.iloc[i].Data_source} data',
#                        'byline':""}
#         }
#         dw.update_metadata(ref.iloc[i]['DW_id_abs'],model_props_abs)
#         # and update the title on a custom basis
#         dw.update_chart(ref.iloc[i]['DW_id_abs'],f"{ref.iloc[i]['Title']} (Total Votes)")
        # and also update the timestamp + vote totals
        dw.update_metadata(ref.iloc[i]['DW_id_abs'],time_update)
        # publish the chart
        dw.publish_chart(ref.iloc[i]['DW_id_abs'])
        print(f"Abs chart published at {getSnapshotTime()}")

        ### UPDATE OR CREATE PERCENTAGE CHART ###

        # whatever we do, add a bottom row to the data
        # first get the dataframe
        pct_data = pct_agg_dict[ref.iloc[i]['Key']].copy()
        # add a column in pct_data for grouping
        pct_data['group'] = 'By group'
        # join with the bottom row data
        pct_data = pd.concat([pct_data,\
                             bottom.rename(columns={'index':ref.iloc[i]['Key']})],ignore_index=True,axis=0)

        # if chart ID exists, update just data
        if len(ref.iloc[i]['DW_id_pct'])==5:
            dw.add_data(ref.iloc[i]['DW_id_pct'],pct_data)
            print(f"Pct chart data updated at {getSnapshotTime()}")

        # otherwise create the chart
        else:
            sub_chart_pct = dw.create_chart(
                title = f"{ref.iloc[i]['Title']} (Percentage)", 
                chart_type = 'd3-bars-stacked',
                data = pct_data,
                folder_id = '159754'
            )
            # update the value in the reference sheet
            ref.iloc[i]['DW_id_pct'] = sub_chart_pct['id']
            print(f"No pct chart id found, new chart created at {getSnapshotTime()}, the ID is {ref.iloc[i]['DW_id_pct']}")

        # either way, update all the metadata
#         model_props_pct = {
#             'visualize':model_pct_vis,
#             'describe':{'source-name':f'Inquirer analysis by {ref.iloc[i].Byline.upper()} of {ref.iloc[i].Data_source} data',
#                        'byline':""}
#         }
#         dw.update_metadata(ref.iloc[i]['DW_id_pct'],model_props_pct)
#         # and update the title on a custom basis
#         dw.update_chart(ref.iloc[i]['DW_id_pct'],f"{ref.iloc[i]['Title']} (Percentage)")
#         # and REMOVE the timestamp
#         dw.update_metadata(ref.iloc[i]['DW_id_pct'],{'annotate' : {'notes':""}})
        # publish the chart
        dw.publish_chart(ref.iloc[i]['DW_id_pct'])
        print(f"Pct chart published at {getSnapshotTime()}")

        print("---")
        
    print(f"All charts updated at {getSnapshotTime()}")

    return ref