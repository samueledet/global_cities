'''
This function gives the patents information for each country office and equivalent patents in US, EU and JP.

Args: countrycode
Result: country_patent
'''

import numpy as np
import pandas as pd
import subprocess as sp
import re, glob
from src.utils import load_pickle, dump_pickle



def merge_raw_dataset(rawdata, outputpath):
    '''This merges the text files in the raw dataset.
    Args: 
    name of the rawdata directory, output path.
    
    '''
    
    filename = glob.glob('/storage/samuel.edet/Patent/global_city/data/raw/%s/*.txt'%(rawdata))
    dataset = []
    
    for file in filename:
        dataset.append(pd.read_csv(file))
    
    output = pd.concat(tls226)
    dump_pickle(output, outputpath)
    
    
def make_data(data1,data2, key):
    '''
    This function merges the different processed dataset together e.g. tls201, tls206, tls207, tls226
    '''
    make_data = pd.merge(data1, data2, how='left', on=str(key)).drop_duplicates(subset=list(data1.columns))
    dump_pickle(make_data, '/home/samuele.edet/global_city_patent/data/processed/%s_%s.p' %(data1, data2))


    
    
def split_data(inventors, assignees,assigneeinventors):
    '''
    This function splits the processed dataset into three categories: inventors only, assignee only and inventors that are assignees.
    
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
    '''
    This function extracts all country patents (and equivalent)
    Args: countrycode(str)
    '''
    
    authorities = ['AE', 'AR', 'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CN', 'CO', 'CZ', 'DE', 'DK', 'ES', 'FI', 'FR', 'GB', 'GR', 'HK', 'HU', 'IE', 'IL', 
                   'IN', 'IT', 'JP', 'KR', 'LU', 'MX', 'MY', 'NL', 'NO', 'NZ', 'PL', 'PT', 'RU', 'SA', 'SE', 'SG', 'TH', 'TR', 'TW', 'ZA', 'US']
    
    
    dataset = load_pickle('/storage/samuel.edet/Patent/global_city/data/processed/tls20176_version1.p')
    dataset = dataset.loc[dataset['appln_auth'].isin(authorities)]
    dataset = dataset.loc[dataset['person_ctry_code'].isin(authorities)]    
    dataset = dataset.reset_index(drop=True)
    
    dataset = dataset.groupby(['docdb_family_id'])
    
    
    
    country_dataset = pd.concat([dataset.get_group(key) for key in tqdm(dataset.groups.keys()) if any(dataset.get_group(key)['appln_auth']==countrycode)])
    
    country_inventor_dataset = country_dataset.query('invt_seq_nr > 0 & applt_seq_nr == 0')
    country_assignee_dataset = country_dataset.query('invt_seq_nr == 0 & applt_seq_nr > 0')
    country_inventor_assignee_dataset = country_dataset.query('invt_seq_nr > 0 & applt_seq_nr > 0')

           
    dump_pickle(country_inventor_dataset, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_inventor.p'%(countrycode))
    dump_pickle(country_assignee_dataset, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_assignee.p'%(countrycode))
    dump_pickle(country_inventor_assignee_dataset, '/storage/samuel.edet/Patent/global_city/data/interim/country_patent/%s_inventorassignee.p'%(countrycode))
    
    return country_inventor_dataset, country_assignee_dataset, country_inventor_assignee_dataset
