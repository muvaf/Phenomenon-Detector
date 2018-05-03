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

      tempIntersection.put(id, aIntersectionList ++ Seq(b.id))
    }
    Node(
      a.id,
      a.followers ++ b.followers ++ tempIntersection,
      Array(b.id) ++ a.subNodes
      )
  }

  def getIntersectionSet(a: Node, b: Node): Set[String] ={
    val unifiedMap = a.followers ++ b.followers

    val onlyNonIntersectedB = b.followers -- a.followers.keySet
    val onlyNonIntersectedA = a.followers -- b.followers.keySet

    val aExcluded = unifiedMap -- onlyNonIntersectedA.keySet
    val allExcluded = aExcluded -- onlyNonIntersectedB.keySet

    allExcluded.keySet
  }

  def print(node: Node): Unit ={
    println("ID: " + node.id)
    println("Coverage: " + node.followers.size)
    println("Subnodes:")
    node.subNodes.foreach(println)
    node.followers.values.foreach(_.foreach(intersectNode => println("Intersects:" + intersectNode)))
//    println("Followers:")
//    node.followers.foreach{ (follower) =>
//      println(follower._1 + " -> " + follower._2)
//    }
  }

  def getNodePairFromRow(row: Row, spark: SparkSession): (Node, Node)={
      (
        Node(row.getAs[String](0), row.getAs[Map[String, Seq[String]]](1), row.getAs[Seq[String]](2)),
        Node(row.getAs[String](3), row.getAs[Map[String, Seq[String]]](4), row.getAs[Seq[String]](5))
      )

  }

}