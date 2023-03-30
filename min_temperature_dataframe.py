from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('temp').getOrCreate()

schema = StructType([
    StructField('stationID', StringType(), True),
    StructField('date', IntegerType(), True),
    StructField('measure_type', StringType(), True),
    StructField('temperature', FloatType(), True)
])

df = spark.read.schema(schema).csv('1800.csv')
df.printSchema()

min_temps = df.filter(df.measure_type == 'TMIN')

station_temps = min_temps.select('stationID', 'temperature')

min_temps_by_station = station_temps.groupBy('stationID').min('temperature')
min_temps_by_station.show()

min_temps_by_station_F = min_temps_by_station.withColumn('temperature',
                                                         func.round(
                                                             func.col('min(temperature)') \
                                                                 * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                                     .select('stationID', 'temperature').sort('temperature')
                                                                     
results = min_temps_by_station_F.collect()

for result in results:
    print(result[0] + '\t{:.2f}F'.format(result[1]))
    
spark.stop()