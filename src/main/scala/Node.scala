import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

case class Node(id: String, followers: Map[String, Seq[String]] = Map.empty, subNodes: Seq[String] = Seq.empty)

object Node {

  def mergeNodes(a: Node, b: Node): Node ={
    val intersectionSet = getIntersectionSet(a, b)
    unifyNodes(a, b ,intersectionSet)
  }

  def unifyNodes(a: Node, b: Node, intersectionSet: Set[String]): Node ={
    val tempIntersection = mutable.Map.empty[String, Seq[String]]

    intersectionSet.foreach { case (id: String) =>
      // id's are guaranteed to exist since we're iterating over the intersection
      val aIntersectionList = a.followers(id)
      val bIntersectionList = b.followers(id)

      tempIntersection.put(id, aIntersectionList ++ bIntersectionList)
    }
    Node(
      a.id,
      a.followers ++ b.followers ++ tempIntersection,
      a.subNodes ++ Array(b.id)
    )
  }

  def getIntersectionSet(a: Node, b: Node): Set[String] ={
    val unifiedMap = a.followers.keySet ++ b.followers.keySet

    val onlyNonIntersectedB = b.followers.keySet -- a.followers.keySet
    val onlyNonIntersectedA = a.followers.keySet -- b.followers.keySet

    val aExcluded = unifiedMap -- onlyNonIntersectedA
    val allExcluded = aExcluded -- onlyNonIntersectedB

    allExcluded
  }

  def print(node: Node): Unit ={
    println("ID: " + node.id)
    println("Coverage: " + node.followers.size)
    println("Subnodes:")
    node.subNodes.foreach(println)
  }

  def getNodePairFromRow(row: Row, spark: SparkSession): (Node, Node)={
      (
        Node(row.getAs[String](0), row.getAs[Map[String, Seq[String]]](1), row.getAs[Seq[String]](2)),
        Node(row.getAs[String](3), row.getAs[Map[String, Seq[String]]](4), row.getAs[Seq[String]](5))
      )

  }

}