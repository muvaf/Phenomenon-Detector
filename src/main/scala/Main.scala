import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    Preprocessor.readTextFile("twitter_combined.txt", spark).show()

    spark.stop()
  }
}
