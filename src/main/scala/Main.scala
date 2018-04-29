import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Phenomenon Detector").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", sc).toDS()
    val seedNode = DiscoveryEngine.extractSeedNode(allNodes, 20, spark)

    val threeMerged = DiscoveryEngine.extractBestMergedNode(seedNode._1, allNodes, spark)
    println("Score: " + threeMerged._2)
    Node.print(threeMerged._1)
    spark.stop()
  }
}
