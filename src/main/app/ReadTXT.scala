import org.apache.spark.sql.SparkSession

object ReadTXT {
  def main(args: Array[String]): Unit = {

    // Create a Spark Session
    // For Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("##spark read text files from a directory into RDD")
    val rddFromFile = spark.sparkContext.textFile(args(0))
    println(rddFromFile.getClass)
    println("##Get data Using collect")
    rddFromFile.collect().foreach(f=>{
      println(f)
    })

}}


