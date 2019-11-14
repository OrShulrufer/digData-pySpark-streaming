#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time


# In[2]:


sc = SparkContext("local[2]", "Or")
ssc = StreamingContext(sc,1)


# In[3]:


#Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)
#Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
#Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x+y)
#Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
#Start the computation
ssc.start()
time.sleep(30)
ssc.stop()


# In[ ]:





# In[ ]:




