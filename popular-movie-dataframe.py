from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)
])

df = spark.read.option('sep', '\t').schema(schema).csv('ml-100k/u.data')

top_movie_id = df.groupby('movie_id').count().orderBy(func.desc('count'))

top_movie_id.show(10)

spark.stop()