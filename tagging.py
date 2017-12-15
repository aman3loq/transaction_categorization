#last edit Aman: 08-Dec-17; 12:57am
import re
import pandas as pd
import numpy as np
import csv
from itertools import repeat
from multiprocessing import cpu_count, Pool
from configobj import ConfigObj


config = ConfigObj('config.ini')
global grammarfile
grammarfile = config['grammarfile']

cores = cpu_count()
partitions = cores
chunk_size = int(config['chunksize'])
delimiter =  config['delimiter']
bk_map_infilename = config['bk_map_infilename']
bk_map_outfilename = config['bk_map_outfilename']
input_file_delimiter = config['input_file_delimiter']
mapped_output_path = config['mapped_output_path']
N = int(config['wc_l']) 


with open(grammarfile, 'r') as gram:
	gram_df = pd.read_csv(gram, sep = ";", names=['gid','key','card','channel','action','merchant','location'])
	sorted_gram_df = gram_df.sort_values(['card'], ascending=False)
	gram_df2 = pd.concat([sorted_gram_df['gid'], sorted_gram_df['key'], sorted_gram_df['channel'], sorted_gram_df['action'], sorted_gram_df['merchant'], sorted_gram_df['location']], axis=1, keys=[ 'gid','key', 'channel', 'action', 'merchant', 'location'])
	gram_df2 = gram_df2.reset_index(drop=True)
	gram_df2['key'] = gram_df2['key'].str.replace(" ","")
	gram_df2.replace(np.nan, '', regex=True, inplace=True)

def matchFunc(chunk):
	ln = str(chunk).strip()
	L_set = set(ln.split(','))

	for j,each in gram_df2['key'].iteritems():
		gram_set=set(each.split(','))
		res=[]
		if gram_set.issubset(L_set):

			res.append(str(gram_df2['gid'][j]))
			break

	return ''.join(res)

def gen_chunks(reader, chunksize=chunk_size):
	""" 
	Chunk generator. Take a CSV `reader` and yield
	`chunksize` sized slices. 
	"""
	chunk = []
	for i, line in enumerate(reader):
		if (i % chunksize == 0 and i > 0):
			yield chunk
			del chunk[:]
		chunk.append(line)
	yield chunk
def matchBatch(chunk):
	chunk['gid'] = chunk.apply(lambda row : matchFunc(row['txn_cleansed']), axis = 1)
	return chunk

def parallelize_dataframe(df, func, n_cores):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


if __name__ == '__main__':

	for offset in range(0, N, chunk_size):
		
		temp = pd.read_csv(bk_map_infilename, sep = '|', quoting = 3, nrows = chunk_size, skiprows = offset, header = None, names = ['id', 'txn_cleansed'])
		print('Read {} lines from offset {}'.format(len(temp), offset))

#		temp['gid'] = temp.apply(lambda row : matchFunc(row['txn_cleansed']), axis = 1)
		temp_result = parallelize_dataframe(temp, matchBatch, cores)
		temp_result = temp_result.merge(gram_df2, on='gid', how='left')
		final_result = temp_result[['id','txn_cleansed','channel','action','merchant','location']]
		final_result.to_csv(mapped_output_path + 'yLabels_out_' + str(offset) + '.psv', sep = '|', index = False)




