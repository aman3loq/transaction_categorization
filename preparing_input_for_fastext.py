#last edit Aman: 12-Dec-17; 16:32pm

#make sure pandas is upgraded to pandas -- 0.21.0

import re
import pandas as pd
import numpy as np
from multiprocessing import cpu_count, Pool
from configobj import ConfigObj
import csv

'''delimiter = ','
cores = cpu_count()
partitions = cores
chunksize=100
'''

def cleanFunc(L):
    d = L[0]
    delimiter = L[1]
    
    d['tran_particular'] = d.apply(lambda x : re.sub('[^a-zA-z ]+',' ', str(x['tran_particular']).lower()), axis = 1)
    d['tran_particular'] = d.apply(lambda x : re.sub('\w\_',' ',str(x['tran_particular']).lower()), axis = 1)
    d['tran_particular'] = d.apply(lambda x : re.sub('\W+', ' ', x['tran_particular']).strip(), axis = 1)
    return d



def parallelize(data, func, delimiter):
    data_split = np.array_split(data, partitions)
    pool = Pool(cores)
    args = []
    delimiter = delimiter
    for each in data_split:
        args.append([each,delimiter])
    data = pd.concat(pool.map(cleanFunc, args))
    pool.close()
    pool.join()
    return data


if __name__ == '__main__':
    config = ConfigObj('config.ini')
    cores = cpu_count()
    partitions = cores
    chunksize = int(config['chunksize'])
    inputfilename = config['final_outfile']
    txn_labeled = config['txn_labeled']
    #txn_labeled_tmp = config['txn_labeled_tmp']
    txn_labeled_id = config['txn_labeled_id']
    txn_unlabeled = config['txn_unlabeled']
    txn_unlabeled_id = config['txn_unlabeled_id']
    delimiter = config['delimiter']

    with open(inputfilename,'r') as infile, open(txn_labeled,'a') as out_l, open(txn_unlabeled,'a') as out_u, open(txn_labeled_id,'a') as out_l_id, open(txn_unlabeled_id,'a') as out_u_id:
        i=0
        for chunk in pd.read_csv(infile, delimiter = delimiter, chunksize=chunksize, header=0, names=['id','tran_particular','channel','action','merchant','location'], error_bad_lines=False, index_col=False, quoting=csv.QUOTE_NONE, encoding='utf-8'):
            if len(chunk) >= 1:
                i=i+1
                print("processing chunk", str(i), "for input to fasttext")
                processed_data = parallelize(chunk, cleanFunc, delimiter)
                
                mask = processed_data[['channel','action','merchant','location']].isnull().all(axis=1)
                df1 = processed_data[~mask]
                df2 = processed_data[mask]
                
                m1 = df1['channel'].notna()
                df1.loc[m1, 'channel'] = "__label__" +df1.loc[m1, 'channel'].astype(str)
                
                m2 = df1['action'].notna()
                df1.loc[m2, 'action'] = "__label__" +df1.loc[m2, 'action'].astype(str)
                
                m3 = df1['merchant'].notna()
                df1.loc[m3, 'merchant'] = "__label__" +df1.loc[m3, 'merchant'].astype(str)
                
                m4 = df1['location'].notna()
                df1.loc[m4, 'location'] = "__label__" +df1.loc[m4, 'location'].astype(str)
                                
                df1_labeled = df1[['channel','action','merchant','location','tran_particular']]
                df1_labeled_id = df1[['id']]
                df2_unlabeled = df2[['tran_particular']]
                df2_unlabeled_id = df2[['id']]

                df1_labeled.to_csv(out_l, mode = 'a', sep = '\t', index = False, header = False, quoting = csv.QUOTE_MINIMAL)
                df1_labeled_id.to_csv(out_l_id, mode = 'a', index = False, header = False)
                df2_unlabeled.to_csv(out_u, mode = 'a', sep = '\t', index = False, header = False, quoting = csv.QUOTE_MINIMAL)
                df2_unlabeled_id.to_csv(out_u_id, mode = 'a', index = False, header = False)
            

                    

