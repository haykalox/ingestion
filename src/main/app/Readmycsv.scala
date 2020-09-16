import org.apache.spark.sql.SparkSession
import scala.io.Source

object Readmycsv extends App {
  override def main(args: Array[String]): Unit = {

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
      .load(args(0))
    println("Created Spark Session")

    df.show()
    df.write.format("csv").save(args(1))

  }
}


