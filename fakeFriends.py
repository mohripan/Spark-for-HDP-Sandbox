from pyspark import SparkContext, SparkConf

def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

conf = SparkConf().setMaster('local').setAppName('fakeFriends')
sc = SparkContext(conf = conf)

lines = sc.textFile('fakeFriends\fakefriends.csv')
rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

print('Results:')
print(totals_by_age)