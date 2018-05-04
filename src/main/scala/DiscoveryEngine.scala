import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DiscoveryEngine {

  def extractSeedNode(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): (Node, Double) ={
    import spark.implicits._

    val bestNodes = spark.createDataFrame(
      allNodes.rdd.takeOrdered(numberOfNodes)(new Ordering[Node]() {
      override def compare(x: Node, y: Node): Int =
        Ordering[Int].compare(y.followers.size, x.followers.size)
    })
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

  def getScore(mergedNode: Node): (Node, Double)={
    val degreeOfIntersection = mergedNode.followers.values.map(_.size).sum.toDouble
    val degreeOfCoverage = mergedNode.followers.size.toDouble

    if (degreeOfIntersection == 0){
      (mergedNode, degreeOfCoverage * degreeOfCoverage / 1)
    }
    else {
      (mergedNode, degreeOfCoverage * degreeOfCoverage / degreeOfIntersection)
    }
  }

  private def getMaxScoredNode(scores: Dataset[(Node, Double)]): (Node, Double)={
    scores.rdd.max()(new Ordering[Tuple2[Node, Double]]() {
      override def compare(x: (Node, Double), y: (Node, Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })
  }

}
