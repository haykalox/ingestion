import org.apache.spark.sql.SparkSession

object Readmycsv extends App {

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

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("c:\\a\\test.csv")
    println("Created Spark Session")

    df.show()
    df.write.format("csv").save("samplesq")

  }



