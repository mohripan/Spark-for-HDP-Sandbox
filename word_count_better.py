import re
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('word_count_better')
sc = SparkContext(conf = conf)

def normalize_word(word):
    return re.compile(r'\W+', re.UNICODE).split(word.lower())

input = sc.textFile('Book.txt')
words = input.flatMap(normalize_word)
word_counts = words.countByValue()

for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, count)