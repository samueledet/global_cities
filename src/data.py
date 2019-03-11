'''
This function gives the patents information for each country office and equivalent patents in US, EU and JP.

Args: countrycode
Result: country_patent
'''

import numpy as np
import pandas as pd
import subprocess as sp
import re
from src.utils import load_pickle, dump_pickle

def make_data(data1,data2, key):
    make_data = pd.merge(data1, data2, how='left', on=str(key)).drop_duplicates(subset=list(data1.columns))
    dump_pickle(make_data, '/home/samuele.edet/global_city_patent/data/processed/%s_%s.p' %(data1, data2))


    
    
def split_data(inventors, assignees,assigneeinventors):
    '''
    This function splits the dataset into three categories: inventors only, assignee only and inventors that are assignees.
    
    Arg
    '''
    
    dataset = load_pickle('/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version1.p')
    
    inventors_only = dataset.query('invt_seq_nr > 0 & applt_seq_nr == 0')
    assignees_only = dataset.query('invt_seq_nr == 0 & applt_seq_nr > 0')
    inventors_assignees = dataset.query('invt_seq_nr > 0 & applt_seq_nr > 0')
    
    dump_pickle(inventors_only, '/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version1_%s.p'%(inventors))
    dump_pickle(assignees_only, '/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version1_%s.p'%(assignees))
    dump_pickle(inventors_assignees, '/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version1_%s.p'%(assigneesinventors))
    
    
    
    
def countrypatent(countrycode):
    inventor_dataset = load_pickle('/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version_1_inventor_only.p')
    assignee_dataset = load_pickle('/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version_1_assignee_only.p')
    inventor_assignee_dataset = load_pickle('/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version_1_assignee_inventor.p')
    
    
    authorities = ['AE', 'AR', 'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CN', 'CO', 'CZ', 'DE', 'DK', 'ES', 'FI', 'FR', 'GB', 'GR', 'HK', 'HU', 'IE', 'IL', 
                   'IN', 'IT', 'JP', 'KR', 'LU', 'MX', 'MY', 'NL', 'NO', 'NZ', 'PL', 'PT', 'RU', 'SA', 'SE', 'SG', 'TH', 'TR', 'TW', 'ZA', 'US']
    
    
    
    inventor_dataset = inventor_dataset.loc[inventor_dataset['appln_auth'].isin(authorities)]
    inventor_dataset = inventor_dataset.loc[inventor_dataset['person_ctry_code'].isin(authorities)]    
    inventor_dataset = inventor_dataset.reset_index(drop=True)
    
    assignee_dataset = assignee_dataset.loc[assignee_dataset['appln_auth'].isin(authorities)]
    assignee_dataset = assignee_dataset.loc[assignee_dataset['person_ctry_code'].isin(authorities)]    
    assignee_dataset = assignee_dataset.reset_index(drop=True)
    
    
    inventor_assignee_dataset = inventor_assignee_dataset.loc[inventor_assignee_dataset['appln_auth'].isin(authorities)]
    inventor_assignee_dataset = inventor_assignee_dataset.loc[inventor_assignee_dataset['person_ctry_code'].isin(authorities)]    
    inventor_assignee_dataset = inventor_assignee_dataset.reset_index(drop=True)
    
    
    
    
    coountry_inventor_patent = pd.concat([inventor_dataset.get_group(key) for key in inventor_dataset.groups.keys() if any(inventor_dataset.get_group(key)['appln_auth']==str(countrycode))])
    coountry_assignee_patent = pd.concat([assignee_dataset.get_group(key) for key in assignee_dataset.groups.keys() if any(assignee_dataset.get_group(key)['appln_auth']==str(countrycode))])
    coountry_inventor_assignee_patent = pd.concat([inventor_assignee_dataset.get_group(key) for key in inventor_assignee_dataset.groups.keys() if any(inventor_assignee_dataset.get_group(key)['appln_auth']==str(countrycode))])

       
    dump_pickle(country_inventor_patent, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_inventor.p'%(countrycode))
    dump_pickle(coountry_assignee_patent, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_assignee.p'%(countrycode))
    dump_pickle(coountry_inventor_assignee_patent, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_invassignee.p'%(countrycode))
    
    return coountry_inventor_patent, coountry_assignee_patent, coountry_inventor_assignee_patent