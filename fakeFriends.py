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

for key, value in rdd.iteritems():
    print(key, value)