import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val list = Preprocessor.readTextFile("twitter_combined.txt", sc).collect()
    list.foreach(println(_))

    spark.stop()
  }
}
