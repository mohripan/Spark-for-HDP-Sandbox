from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, asc, desc

spark = SparkSession.builder.appName('word-count').getOrCreate()

inputDF = spark.read.text('Book.txt')

words = inputDF.select(func.explode(func.split(inputDF.value, '\\W+')).alias('word'))
words.filter(words.word != '')

lowercase_words = words.select(func.lower(words.word).alias('word'))

word_counts = lowercase_words.groupBy('word').count()

word_counts_sorted = word_counts.sort(col('count').desc())

word_counts_sorted.show()