import scala.collection.mutable

case class Node(id: String, followers: Map[String, Int] = null, subNodes: Option[Set[String]] = None)

object Node {

  def mergeNodes(a: Node, b: Node): Node ={
    val intersectionSet = getIntersectionSet(a, b)
    unifyNodes(a, b ,intersectionSet)
  }

  def unifyNodes(a: Node, b: Node, intersectionSet: Set[String]): Node ={
    val tempIntersection = mutable.Map.empty[String, Int]

    intersectionSet.foreach { case (id: String) =>
      val aCount = a.followers.getOrElse(id, 0)
      val bCount = b.followers.getOrElse(id, 0)

      tempIntersection.put(id, aCount + bCount + 1)
    }
    Node(
      a.id,
      a.followers ++ b.followers ++ tempIntersection,
      Option(
        Set(b.id, a.id) ++ a.subNodes.getOrElse(Set.empty) ++ b.subNodes.getOrElse(Set.empty)
      ))
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
    println("Subnodes:")
    node.subNodes.foreach(println)
    println("Followers:")
    node.followers.foreach{ (follower) =>
      println(follower._1 + " -> " + follower._2)
    }
  }

}