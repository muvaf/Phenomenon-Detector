import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", sc)

    val bestNode = DiscoveryEngine.extractBestNode(allNodes, 1)
    println("Score: " + bestNode._2)
    Node.print(bestNode._1)

    spark.stop()
  }
}
