import org.apache.spark.rdd.RDD

object DiscoveryEngine {

  def extractBestNode(allNodes: RDD[Node], numberOfNodes: Int): (Node, Int) ={
    val mergerScores = allNodes.collect().map{ (theNode) =>
      getMergerScores(theNode, allNodes)
    }
    println("NUMBER OF RDDS: " + mergerScores.size)
    val unionedMergerScores = mergerScores.map(_.collect()).reduceLeft(_ ++ _)

    unionedMergerScores.maxBy(_._2)
  }

  def getMergerScores(theNode: Node, allNodes: RDD[Node]): RDD[(Node, Int)] ={
    allNodes.map { (oneNode) =>
      val mergedNode = Node.mergeNodes(theNode, oneNode)
      val degreeOfIntersection = mergedNode.followers.values.sum
      val degreeOfCoverage = mergedNode.followers.size

      if (degreeOfIntersection == 0){
        (oneNode, degreeOfCoverage * degreeOfCoverage / 1)
      }
      else {
        (oneNode, degreeOfCoverage * degreeOfCoverage / degreeOfIntersection)
      }
    }
  }

}
