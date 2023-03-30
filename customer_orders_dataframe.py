from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName('customer_order').getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('item_id', IntegerType(), True),
    StructField('price', FloatType(), True)
])

df = spark.read.schema(schema).csv('customer-orders.csv')

total_amount = df.select('id', 'price')
total_amount = total_amount.groupBy('id').agg(func.round(func.sum('price'), 2).alias('total_spent'))
total_amount = total_amount.sort('total_spent')
total_amount.show()

spark.stop()