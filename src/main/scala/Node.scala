import scala.collection.mutable

case class Node(id: String, followingList: Map[String, Int] = null, subNodes: Option[Set[String]] = None)

object Node {

  def getIntersectedNode(a: Node, b: Node): Node ={
    val intersectionSet = getIntersectionSet(a, b)
    unifyNodes(a, b ,intersectionSet)
  }

  def unifyNodes(a: Node, b: Node, intersectionSet: Set[String]): Node ={
    val tempIntersection = mutable.Map.empty[String, Int]

    intersectionSet.foreach { case (id: String) =>
      val aCount = a.followingList.getOrElse(id, 0)
      val bCount = b.followingList.getOrElse(id, 0)

      tempIntersection.put(id, aCount + bCount + 1)
    }
    Node(
      a.id,
      a.followingList ++ b.followingList ++ tempIntersection,
      Option(
        Set(b.id, a.id) ++ a.subNodes.getOrElse(Set.empty)
      ))
  }

  def getIntersectionSet(a: Node, b: Node): Set[String] ={
    val unifiedMap = a.followingList ++ b.followingList

    val onlyNonIntersectedB = b.followingList -- a.followingList.keySet
    val onlyNonIntersectedA = a.followingList -- b.followingList.keySet

    val aExcluded = unifiedMap -- onlyNonIntersectedA.keySet
    val allExcluded = aExcluded -- onlyNonIntersectedB.keySet

    allExcluded.keySet
  }

}