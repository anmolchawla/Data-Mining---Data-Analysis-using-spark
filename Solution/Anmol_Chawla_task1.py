import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


ip_csv_file = sys.argv[1]
op_csv_file = sys.argv[2]


df = sqlContext.read.options(header='true', inferschema='true').csv(ip_csv_file)
data = df.drop("userId","timestamp").sort('movieId').groupby('movieId').avg('rating').select(col("movieId").alias("movieId"),col("avg(rating)").alias("rating_avg")).coalesce(1).write.options(header="true").csv(op_csv_file)