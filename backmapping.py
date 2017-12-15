#backmap to raw_dataset

import pandas as pd
import csv
from configobj import ConfigObj

config = ConfigObj('config.ini')
chunksize = int(config['chunksize'])

N = int(config['wc_l'])

rawfile = config['rawfile']
tag_files_path = config['tag_files_path']
final_outfile = config['final_outfile']
sepr = config['input_file_delimiter']

files=[]

for i in range(0,N,chunksize):
    files.append(tag_files_path+"yLabels_out_"+str(i)+".psv")

for i, x in enumerate(pd.read_csv(rawfile, sep = sepr, index_col=['ID'], chunksize=chunksize, error_bad_lines = False, quoting=csv.QUOTE_NONE)):
    #added try for avoid errors if want seelct non exist file in list files
    #print(i,x)
    print("back mapping tags to raw data chunk "+str(i))
    try:
        df = pd.read_csv(files[i], index_col=['id'],  sep = sepr,error_bad_lines = False,quoting=csv.QUOTE_NONE)
        #df1 = pd.concat([x, df['tag']], axis=1)
        df1 = pd.concat([x, df['channel'], df['action'], df['merchant'], df['location']], axis=1)
        #print (df1)
        #in first loop create header in output
        if i == 0:
            pd.DataFrame(columns=df1.columns).to_csv(final_outfile)
            
        #remove ','(commas) from raw files as they are used as delimiters for final output file
        df1 = df1.astype(str).replace(',',' ')
        #append data to output file
        df1.to_csv(final_outfile, mode='a', header=False)
        
    except IndexError as e:
        print ('no files in list')
