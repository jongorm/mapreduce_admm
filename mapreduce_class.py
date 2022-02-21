
#MapReduce
#Partition data
#Map (key, value)
#Shuffle to reducers. Each key goes to the same reducer. REducers can receive multiple keys.
#Reducer aggregates values into a list for identical keys. Then performs a summary operation for each key.

#First step: manually divide data into chunks

import collections
import itertools
import multiprocessing

class MapReduceBasic():
    
    #Class constructor, runs at instantiation
    def __init__(self, map_function, reduce_function, num_workers=None):
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.mp_pools = multiprocessing.Pool(num_workers)
    
    #partition function, run after the mapper and inputted to the reducer
    def group(self, mapped_data):
        grouped_data = collections.defaultdict(list) #If the key when called does not exist, make a key value pair with an empty list
        for key, val in mapped_data:
            grouped_data[key].append(val)
        return grouped_data.items()
    
    #Inputs is an iterable containing the data files to be processed. 
    #This function performs the map reduce with the given functions.
    #https://stackoverflow.com/questions/9663562/what-is-the-difference-between-init-and-call
    def __call__(self, inputs, chunksize=1): #chunksize applies to one file. What if multiple files?
        mapped_response = self.pool.map(self.map_function, inputs, chunksize=chunksize)
        grouped_data = self.group(itertools.chain(*mapped_response)) #chain combines the iterables in the argument. Because partition() takes a dictionary
        reduced_values = self.pool.map(self.reduce_function, grouped_data) # as argument, and mapped responses outputs a list of tuples (possibly multiple), chain combines all the lists
        return reduced_values
        












