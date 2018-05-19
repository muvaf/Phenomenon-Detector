import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Phenomenon Detector").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", sc).toDS()
    var seedNode = DiscoveryEngine.extractSeedNode(allNodes, 300, spark)
    println(seedNode._1)
    val maxNumOfNodes = 9
    var i = 0
    for ( i <- 1 to maxNumOfNodes){
      seedNode = DiscoveryEngine.extractBestMergedNode(seedNode._1, allNodes, spark)
      println(seedNode._1)
    }
    println("Score: " + seedNode._2)
    Node.print(seedNode._1)
    val interAmounts = OutputProcessor.getIntersectionAmounts(seedNode._1)
    OutputProcessor.writeToFile(seedNode._1, interAmounts, "output_v3.txt")
    spark.stop()
  }
}
