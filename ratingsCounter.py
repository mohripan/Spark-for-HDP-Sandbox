from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc = SparkContext(conf = conf)

lines = sc.textFile('ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

print('Result:')
print(result)

sortedResults = collections.OrderedDict(sorted(result.items()))

print('Sorted Results:')
print(sortedResults)

for key, value in sortedResults.items():
    print(key, value)