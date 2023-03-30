from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('fake_friends')
sc = SparkContext(conf = conf)

def parse_line(line):
    fields = line.split(',')
    age = 0
    num_friends = 0
    if int(fields[2]) > 30:
        age = int(fields[2])
        num_friends = int(fields[3])
    return (age, num_friends)

lines = sc.textFile('fakefriends.csv')

rdd = lines.map(parse_line)
filter_age = rdd.mapValues(lambda x: x != 0)
total_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_age = total_age.mapValues(lambda x: x[0] / x[1])
results = average_age.collect()

for result in results:
    print(result)