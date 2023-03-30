from pyspark.sql import SparkSession, Row

spark = SparkSession.appName('SparkSQL').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = (fields[0]),
               name = str(fields[1].encode('utf8')),
               age = int(fields[2]),
               num_friends = int(fields[3]))
    
lines = spark.sparkContext.textFile('fakefriends.csv')
people = lines.map(mapper)

schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView('people')

teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')

for teen in teenagers.collect():
    print(teen)
    
schema_people.groupBy('age').count().orderBy('age').show()
spark.stop()