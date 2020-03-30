import sys
 
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)

#Read data from text file and split each line into words
lowered = sc.textFile(sys.argv[1]).map(lambda s:s.lower().split(" "))

words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))
#Now count the occurrence of each word
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x + y).collectAsMap()

bigram = lowered.flatMap(lambda s:[(s[i], s[i+1])for i in range (len(s)-1)])

bigram = bigram.map(lambda word: (word, 1))
bigramCounts = bigram.reduceByKey(lambda x,y:x + y)
bigramCounts.saveAsTextFile("bigram_Counts")


bigram_freq = bigramCounts.map(lambda x:((x[0][0],x[0]), float(x[1])/wordCounts[x[0][0]]))
bigram_freq.saveAsTextFile("bigrams_frequency")

sc.stop()



