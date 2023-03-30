import re
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('word_count_better_sorted')
sc = SparkContext(conf = conf)

def normalize_word(word):
    return re.compile(r'\W+', re.UNICODE).split(word.lower())

rdd = sc.textFile('Book.txt')
words = rdd.flatMap(normalize_word)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    clean_word = result[1].encode('ascii', 'ignore')
    
    if clean_word:
        print(clean_word, count)