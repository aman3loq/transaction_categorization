#last edit Aman; 05-Dec-17; 12:50pm


from itertools import repeat
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from datetime import datetime
import pandas as pd
import numpy as np
from multiprocessing import cpu_count, Pool
from configobj import ConfigObj
import csv

def cleanFunc(L):
	d = L[0]
	delimiter = L[1]
	cachedStopWords = stopwords.words('english')

	# removal of non alphabetic characters
	d['tran_particular'] = d.apply(lambda x : re.sub('[^a-zA-z ]+',' ', str(x['tran_particular']).lower()), axis = 1)
	d['tran_particular'] = d.apply(lambda x : re.sub('\w\_',' ',str(x['tran_particular']).lower()), axis = 1)
	# removal of extra white space 
	d['tran_particular'] = d.apply(lambda x : re.sub('\W+', ' ', x['tran_particular']).strip(), axis = 1)
	# removing stop words
	# make sure to have nltk data downloaded before-hand
	d['tran_particular'] = d['tran_particular'].apply(lambda x: ' '.join([item for item in x.split() if item not in cachedStopWords]))
	# positional marking
	d['tran_particular'] = d.apply(lambda z : delimiter.join(map(lambda z, y : z +'_P' + str(y), z['tran_particular'].split(),range(1,len(z['tran_particular'].split()) + 1))), axis = 1)

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
	delimiter =  config['delimiter']
	inputfilename = config['inputfilename']
	outputfilename = config['outputfilename']
	input_file_delimiter = config['input_file_delimiter']

	with open(inputfilename,'r') as inputfile, open(outputfilename,'a') as outfile:
		ireader = pd.read_csv(inputfile, delimiter = input_file_delimiter, chunksize = chunksize, iterator = True, error_bad_lines = False, quoting=csv.QUOTE_NONE, encoding='utf-8')
		i=0
		#start_time = datetime.now()
		for chunk in ireader:

			if len(chunk) >= 1:
				i=i+1
				print("processing chunk", str(i))
				processed_data = parallelize(chunk, cleanFunc, delimiter)
				processed_data.to_csv(outfile, mode = 'a', sep = '|', index = False, header = False)
		#end_time = datetime.now()
		#print(end_time - start_time)
