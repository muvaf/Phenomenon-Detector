import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark Test")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext

}