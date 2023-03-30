from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('customer_orders')
sc = SparkContext(conf = conf)

def seperate_lines(line):
    fields = line.split(',')
    id = int(fields[0])
    spent = float(fields[2])
    return (id, spent)

lines = sc.textFile('customer-orders.csv')
rdd = lines.map(seperate_lines).reduceByKey(lambda x, y: x + y)
sorted_rdd = rdd.map(lambda x: (x[1], x[0])).sortByKey()

results = sorted_rdd.collect()

for result in results:
    print(result[1], result[0])

# total = rdd.reduceByKey(lambda x: x[0] + x[1])

# results = total.collect()

# for result in results:
#     print(result)