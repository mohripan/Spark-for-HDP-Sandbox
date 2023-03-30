from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('word_count')
sc = SparkContext(conf = conf)

input = sc.textFile('Book.txt')
words = input.flatMap(lambda x: x.split())
word_counts = words.countByValue()

for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, count)