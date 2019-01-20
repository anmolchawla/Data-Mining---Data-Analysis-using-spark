import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("task2").setSparkHome("src/main/resources")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("task2").getOrCreate;
    val df1 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema","true").load(args(0)).drop("userId", "timestamp")
    val df2 = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema","true").load(args(1)).drop("userId", "timestamp")
    val df = df1.join(df2, df1("movieId") <=> df2("movieId") , "inner").drop("movieId").groupBy("tag").avg("rating").orderBy(desc("tag")).withColumnRenamed("avg(rating)", "rating_avg").coalesce(1).write.format("csv").option("header","true").save(args(2))
  }
}