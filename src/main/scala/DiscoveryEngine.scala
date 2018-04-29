import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DiscoveryEngine {

  def extractSeedNode(allNodes: Dataset[Node], numberOfNodes: Int, spark: SparkSession): (Node, Int) ={
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

  def extractBestMergedNode(seedNode: Node, allNodes: Dataset[Node], spark: SparkSession): (Node, Int) ={
    import spark.implicits._
    val scores = allNodes.map{ (otherNode) =>
      getMergerScore(seedNode, otherNode)
    }
    getMaxScoredNode(scores)
  }

  private def getMergerScore(aNode: Node, bNode: Node): (Node, Int)={
    val mergedNode = Node.mergeNodes(aNode, bNode)
    val degreeOfIntersection = mergedNode.followers.values.sum
    val degreeOfCoverage = mergedNode.followers.size

    if (degreeOfIntersection == 0){
      (mergedNode, degreeOfCoverage * degreeOfCoverage / 1)
    }
    else {
      (mergedNode, degreeOfCoverage * degreeOfCoverage / degreeOfIntersection)
    }
  }

  private def getMaxScoredNode(scores: Dataset[(Node,Int)]): (Node, Int)={
    scores.rdd.max()(new Ordering[Tuple2[Node, Int]]() {
      override def compare(x: (Node, Int), y: (Node, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })
  }

}
