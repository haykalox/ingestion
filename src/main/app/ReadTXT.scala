import org.apache.spark.sql.{Dataset, SparkSession}

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


    val df:Dataset[String] = spark.read.textFile(args(0))
    df.printSchema()
    df.collect.foreach(println)

    // val op= df.rdd.map(_.toString()).saveAsTextFile(args(1))

  }
}


