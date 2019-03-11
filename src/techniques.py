'''
The techniques involves: 
1. GROUPBY; 
     - docdb_family_id and doc_std_name 
     - appln_auth and person_id
2. SIMILARitY; 
    - docdb_family_id
    - appln_filing year
'''

import numpy as np
import pandas as pd
import pickle, difflib
import subprocess as sp



def groupby(countrypatent, variable1, variable2):
    
    '''
    Args: Countrypatent, variable1 e.g. docdb_family_id, variable2 e.g. doc_std_name
    Result: filled missing address of countrypatent
    '''
    
    group = countrypatent.groupby([str(variable1),str(variable2)])
    store_keys_table = [group.get_group(key) for key in group.groups.keys()]
    
    for x in range(len(store_keys_table)):
        docdf = pd.DataFrame(store_keys_table[x])
        if docdf['person_address'].isnull().any() == False or docdf.shape[0] == 1:
            continue 
        else:
            if pd.isnull(docdf.iloc[y]['person_address']) == False:
                address = docdf.iloc[y]['person_address']
                docdf['person_address'] = docdf['person_address'].fillna(address)
                store_keys_table[x] = docdf
                break
            else:
                continue
                    
    groupbytable = pd.concat(store_keys_table)
    
    return groupbytable




def similarity(groupbytable, variable3):
    
    '''
    This function fill missing address using similarity techniques:
    
    Args: groupbytable, variable3 e.g. docdb_family_id, 'appln_filing_year'
    Result: similaritytable
    '''
    
    group = groupbytable.groupby([str(variable3)])
    
    store_keys_table = [group.get_group(key) for key in group.groups.keys()]
    
    for x in range(len(store_keys_table)):
        docdf  = pd.DataFrame(store_keys_table[x])
        if docdf['person_address'].isnull().any() == False or docdf.shape[0]==1:
            continue
        else:
            possibilities = list(set([docdf.iloc[j]['doc_std_name'] for j in range(docdf.shape[0]) 
                                      if pd.isnull(docdf.iloc[j]['person_address']) == False]))

            for i in range(docdf.shape[0]):
                match_list = difflib.get_close_matches(docdf.iloc[i]['doc_std_name'], possibilities, n=1, cutoff=0.6)
                if pd.isnull(docdf.iloc[i]['person_address']) == True and len(match_list)>0:

                    match = match_list[0]
                    address = docdf.loc[docdf['doc_std_name'] == match, 'person_address']
                    
                    docdf.loc[docdf['doc_std_name'] == docdf.iloc[i]['doc_std_name'],'person_address'] = address.iloc[0]
                    docdf.update(docdf)
                else:
                    continue

            store_keys_table[x] = docdf
    
    similaritytable = pd.concat(store_keys_table)
    
    return similaritytable




def coverage(filledtable):
    
    '''
    Args: filedtable e.g. countrypatent, groupbytable, similaritytable.
    Results: coverage statistics.
    '''
    
    after_coverage = filledtable.groupby('appln_auth')['appln_id','person_id','person_address'].count()
    after_coverage['address_coverage(%)'] = (after_coverage['person_address']/after_coverage['person_id'])*100
    total_after = (after_coverage['person_address'].sum() / after_coverage['person_id'].sum())*100
    
    return after_coverage, total_after
    
    
    
