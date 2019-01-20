import org.apache.spark.{SparkConf, SparkContext}

object task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("task1").setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("task1").getOrCreate;
    val df = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema","true").load(args(0))
    val df1 = df.drop("userId", "timestamp").groupBy("movieId").avg("rating").sort("movieId").withColumnRenamed("avg(rating)", "rating_avg").coalesce(1).write.format("csv").option("header","true").save(args(1))
  }
}
