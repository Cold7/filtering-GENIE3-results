import multiprocessing as mp
import pandas as pd
import argparse

def get_index(target):

	#selecting sub dataframe	
	subDF = df[df[1] == target]

	#computing number of elements that fits the current percentage
	x = int(len(list(subDF[2])) * percentage)
	#get a list of indices for the first x-th elements
	indices = list(subDF.index)[0:x]
	
	return indices

if __name__ == "__main__":
	#global variables
	global filename, percentage, df
	
	#arguments of the script
	parser = argparse.ArgumentParser("Script for filtering GENIE3 results. For each target gene, it compute the best scores (for example the best 10%) of TFs that direct it expression.\n")
	parser.add_argument("-i", "--input", help="Path to the GENIE3 result file (with format TF\ttarget\tscore)", required=True)
	parser.add_argument("-p","--percentage",help="Percentage of best results to extract. Default: 0.1", default=0.1, type=float)
	parser.add_argument("-o","--output", help="Output path to save results", default = "./genie3_filtered.tsv")
	parser.add_argument("-n","--nproc",help="Number of processors to use. Default: 1", default=1, type=int)
	
	args = parser.parse_args()
	
	#starting script
	filename = args.input
	percentage = args.percentage
	
	#loading dataframe
	df = pd.read_csv(filename, sep="\t", header=None)
	
	#drop no interactions
	df.drop(df[df[2] == 0].index, inplace=True)
	df.reset_index(inplace=True)
	
	#make a list of targets
	targets = set(list(df[1]))	

	pool = mp.Pool(processes=args.nproc)
	listOfIndices = pool.map(get_index, targets, chunksize=1)
	
	#list to save indices
	indices = []
	for indices_ in listOfIndices:
		for index in indices_:
			indices.append(index)
	finalDF = df.iloc[indices]
	del finalDF["index"]
	finalDF.to_csv(args.output, index = None, header = None, sep = "\t")
