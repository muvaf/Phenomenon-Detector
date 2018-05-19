import org.apache.spark.sql.{Dataset, SparkSession}

object Experiments {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Experiments").master("local[*]").getOrCreate()
    import spark.implicits._
    val allNodes = Preprocessor.readTextFile("twitter_combined.txt", spark.sparkContext).toDS()
    getHighestFollowerNodes(allNodes, 11, spark)
    //val nodes = List(115485051,40981798,813286,15913,11348282,7861312,3359851,17093617,79797834,15853668,972651).map(_.toString)
    //getResultsForGivenNodeList(allNodes, nodes, spark)
    spark.stop()
  }

  def getHighestFollowerNodes(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): Unit ={
    val bestOnes = allNodes.rdd.takeOrdered(numberOfNodes)(Ordering[Int].reverse.on(node=>node.followers.size))
    bestOnes.foreach{ node =>
      println(node.id + ": " + node.followers.size)
    }
    val mergedNode = bestOnes.reduce(Node.mergeNodes)
    println("merged " + mergedNode.id + ": " + mergedNode.followers.size)
    val interAmounts = OutputProcessor.getIntersectionAmounts(mergedNode)
    OutputProcessor.writeToFile(mergedNode, interAmounts, "best_nodes_11.txt")
  }

  def getResultsForGivenNodeList(allNodes: Dataset[Node], listOfIds: List[String], spark: SparkSession): Unit ={
    val chosenNodes = allNodes.filter(node => listOfIds.contains(node.id)).collect()
    val mergedNode = chosenNodes.reduce(Node.mergeNodes)
    println("merged " + mergedNode.id + ": " + mergedNode.followers.size)
    val interAmounts = OutputProcessor.getIntersectionAmounts(mergedNode)
    OutputProcessor.writeToFile(mergedNode, interAmounts, "given_nodes.txt")
  }
}
