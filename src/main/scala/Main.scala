import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]) {
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Phenomenon Detector").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", sc).toDS()

    //getBestMergedNodes(allNodes, 11, spark)

    getBestMergedNodesUntilCoverage(allNodes, 25000, spark)

    //getHighestFollowerNodes(allNodes, 32, spark)

    spark.stop()
  }

  def getBestMergedNodes(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): Unit ={
    var seedNode = DiscoveryEngine.extractSeedNode(allNodes, 300, spark)
    println("Score: " + seedNode._2 + " Coverage: " + seedNode._1.followers.size + " Number Of Nodes: " + seedNode._1.subNodes.size)
    println("Id: " + seedNode._1.id + " Sub-nodes: " + seedNode._1.subNodes)
    // -2 is for the seed node which already has 2 nodes merged
    val maxNumOfNodes = numberOfNodes - 2
    var i = 0
    for ( i <- 1 to maxNumOfNodes){
      seedNode = DiscoveryEngine.extractBestMergedNode(seedNode._1, allNodes, spark)
      println("Score: " + seedNode._2 + " Coverage: " + seedNode._1.followers.size + " Number Of Nodes: " + seedNode._1.subNodes.size)
      println("Id: " + seedNode._1.id + " Sub-nodes: " + seedNode._1.subNodes)
    }
    Node.print(seedNode._1)
    val interAmounts = OutputProcessor.getIntersectionAmounts(seedNode._1)
    OutputProcessor.writeToFile(seedNode._1, interAmounts, "output_v3_" + numberOfNodes + ".txt")
  }

  def getBestMergedNodesUntilCoverage(allNodes: Dataset[Node], coverage: Int, spark: SparkSession): Unit ={
    var seedNode = DiscoveryEngine.extractSeedNode(allNodes, 300, spark)
    println("Score: " + seedNode._2 + " Coverage: " + seedNode._1.followers.size + " Number Of Nodes: " + seedNode._1.subNodes.size)
    println("Id: " + seedNode._1.id + " Sub-nodes: " + seedNode._1.subNodes)

    while ( seedNode._1.followers.size < coverage ){
      seedNode = DiscoveryEngine.extractBestMergedNode(seedNode._1, allNodes, spark)
      println("Score: " + seedNode._2 + " Coverage: " + seedNode._1.followers.size + " Number Of Nodes: " + seedNode._1.subNodes.size)
      println("Id: " + seedNode._1.id + " Sub-nodes: " + seedNode._1.subNodes)
    }
    Node.print(seedNode._1)
    val interAmounts = OutputProcessor.getIntersectionAmounts(seedNode._1)
    OutputProcessor.writeToFile(seedNode._1, interAmounts, "output_v2_" + coverage + ".txt")
  }

  def getHighestFollowerNodes(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): Unit ={
    val bestOnes = allNodes.rdd.takeOrdered(numberOfNodes)(Ordering[Int].reverse.on(node=>node.followers.size))
    val mergedNode = bestOnes.reduce(Node.mergeNodes)
    val interAmounts = OutputProcessor.getIntersectionAmounts(mergedNode)
    OutputProcessor.writeToFile(mergedNode, interAmounts, "best_nodes_"+ numberOfNodes+ ".txt")
  }

  def getResultsForGivenNodeList(allNodes: Dataset[Node], listOfIds: List[String], spark: SparkSession): Unit ={
    val chosenNodes = allNodes.filter(node => listOfIds.contains(node.id)).collect()
    val mergedNode = chosenNodes.reduce(Node.mergeNodes)
    println("merged " + mergedNode.id + ": " + mergedNode.followers.size)
    val interAmounts = OutputProcessor.getIntersectionAmounts(mergedNode)
    OutputProcessor.writeToFile(mergedNode, interAmounts, "given_nodes.txt")
  }
}
