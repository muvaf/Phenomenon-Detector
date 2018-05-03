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
      // b assumed to have followers with no intersection data because we go one way, so only a has intersection data
      // belonging to its followers
      tempIntersection.put(id, aIntersectionList ++ Seq(b.id))
    }
    Node(
      a.id,
      a.followers ++ b.followers ++ tempIntersection,
      Array(b.id) ++ a.subNodes
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
    node.followers.foreach{ case (id, intersectSequence) =>
      if (intersectSequence.nonEmpty) {
        println("inter: " + id + " " + intersectSequence.reduce(_ + "," + _))
      }
    }
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