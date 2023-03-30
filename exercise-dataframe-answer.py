from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('Solution').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('fakefriends-header.csv')

friends_by_age = people.select('age', 'friends')

friends_by_age.groupBy('age').avg('friends').show()

friends_by_age.groupBy('age').avg('friends').sort('age').show()

friends_by_age.groupBy('age').agg(func.round(func.avg('friends'), 2)).sort('age').show()

friends_by_age.groupBy('age').agg(func.round(func.avg('friends'), 2).alias('friends_avg')).sort('age').show()

spark.stop()