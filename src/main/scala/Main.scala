import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Phenomenon Detector").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", sc).toDS()
    var seedNode = DiscoveryEngine.extractSeedNode(allNodes, 20, spark)

    val maxNumOfNodes = 5
    var i = 0
    for ( i <- 1 until maxNumOfNodes){
      seedNode = DiscoveryEngine.extractBestMergedNode(seedNode._1, allNodes, spark)
    }
    println("Score: " + seedNode._2)
    Node.print(seedNode._1)
    val pairDs = OutputProcessor.getListOfPairs(seedNode._1, allNodes, spark)
    val intersectionAmounts = OutputProcessor.getIntersectionAmount(pairDs)
    OutputProcessor.writeToFile(seedNode._1, intersectionAmounts)

    spark.stop()
  }
}
