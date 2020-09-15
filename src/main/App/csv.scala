import org.apache.spark.sql.SparkSession

object csv {

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

  val df = spark.read
    .format("csv")
    .option("header", "true") //first line in file has headers
    .option("mode", "DROPMALFORMED")
    .load("c:///a/test.csv")




}

