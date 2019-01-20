import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *


sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


ip_file1 = sys.argv[1]
ip_file2 = sys.argv[2]
op_file  =  sys.argv[3]


df1 = sqlContext.read.options(header='true', inferschema='true').csv(ip_file1).drop("timestamp","userId")
df2 = sqlContext.read.options(header='true', inferschema='true').csv(ip_file2).drop("timestamp","userId")



df = df1.join(df2, ['movieId'], how = "inner") \
.drop("movieId")\
.groupby('tag')\
.avg('rating')\
.sort('tag', ascending = False)\
.select(col("tag").alias("tag"),col("avg(rating)").alias("rating_avg"))\
.coalesce(1).write.options(header= "true").csv(op_file)

