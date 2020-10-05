import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

object Readxml {

  def main(args: Array[String]): Unit = {



    val spark = SparkSession.builder.getOrCreate()
    println("-------------------------------------------------------------------")

    val df = spark.read
      .option("rootTag","message")
      .option("rowTag","personne")
      .xml("D:\\test1.xml")

    df.show()

    println("********************************************************************")

  }
}
