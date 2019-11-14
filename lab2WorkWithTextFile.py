#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark import SparkContext


# In[3]:


# 1-a-count word in file
sc = SparkContext("local", "Work with text file")
lines = sc.textFile("text.txt")
words = lines.flatMap(lambda line: line.split(" "))
print(words.count())


# In[4]:


# 1-b-count a number of different words in file
differentWords = words.distinct()
print(differentWords.count())


# In[13]:


# 1-c- pairs (word, number of its occurrences), sorted by number of occurrences of a word in the file
wordsTuples = words.map(lambda word: (word, 1))
differentWordsCount = wordsTuples.reduceByKey(lambda x, y: x + y)
colectedDifferentWordsCount = differentWordsCount.collect()
print(colectedDifferentWordsCount)


# In[27]:


# 2 - calculate pi 
import random
import math

def inside(r):
    return r < 1
 
k = 1000000 # number of points
randomPointsSet = set()

#create points
for i in range(k):
    d = ((random.uniform(-1, 1)), (random.uniform(-1, 1)))
    randomPointsSet.add(d)

randomPointsSetRdd = sc.parallelize(randomPointsSet)
normSetRdd= randomPointsSetRdd.map(lambda u: math.sqrt(u[0]**2 + u[1]**2))
counter = rRdd.filter(inside).count()
pi = (4*counter) / k
print(pi)


# In[ ]:




