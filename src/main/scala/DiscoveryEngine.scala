import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DiscoveryEngine {

  def extractSeedNode(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): (Node, Double) ={
    import spark.implicits._

    val bestNodes = spark.createDataFrame(
      allNodes.rdd.takeOrdered(numberOfNodes)(Ordering[Int].reverse.on(_.followers.size))
    ).as[Node]

    val crossJoinNodes = bestNodes.crossJoin(bestNodes)
    val scores = crossJoinNodes.map{ (row) =>
      val (aNode, bNode) = Node.getNodePairFromRow(row, spark)
      getMergerScore(aNode, bNode)
    }

    getMaxScoredNode(scores)
  }

  def extractBestMergedNode(seedNode: Node, allNodes: Dataset[Node], spark: SparkSession): (Node, Double) ={
    import spark.implicits._
    val scores = allNodes.map{ (otherNode) =>
      getMergerScore(seedNode, otherNode)
    }
    getMaxScoredNode(scores)
  }

  def getMergerScore(aNode: Node, bNode: Node): (Node, Double)={
    val mergedNode = Node.mergeNodes(aNode, bNode)
    getScore(mergedNode)
  }

  def getScore(mergedNode: Node): (Node, Double) ={
    val degreeOfCoverage = mergedNode.followers.size.toDouble
    val degreeOfIntersection = mergedNode.followers.values.map(_.size).sum.toDouble
//    (mergedNode, degreeOfCoverage)
    if (degreeOfIntersection == 0){
      (mergedNode, degreeOfCoverage * degreeOfCoverage / 1)
    }
    else {
      (mergedNode, degreeOfCoverage * degreeOfCoverage / degreeOfIntersection)
    }
  }

  private def getMaxScoredNode(scores: Dataset[(Node, Double)]): (Node, Double)={
    scores.rdd.max()(Ordering[Double].on(_._2))
  }

}
