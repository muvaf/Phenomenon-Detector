import org.scalatest.{FunSuite, Matchers}

class NodeTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the true intersection set") {
    val aNode = Node("10", Map[String, Int]("30" -> 1, "40" -> 1, "50" -> 1, "100" -> 5))
    val bNode = Node("20", Map[String, Int]("30" -> 1, "60" -> 1, "70" ->1, "100" -> 2))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(2)
    intersectionSet should contain("30")
    intersectionSet should contain("100")
  }

  test("Returns empty node if there is no intersection") {
    val aNode = Node("10", Map[String, Int]("150" -> 1, "40" -> 1, "50" -> 1, "110" -> 5))
    val bNode = Node("20", Map[String, Int]("30" -> 1, "60" -> 1, "70" ->1, "100" -> 2))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(0)
  }

  test("Returns unified node") {
    val aNode = Node("10", Map[String, Int]("30" -> 1, "40" -> 1, "50" -> 1, "100" -> 2))
    val bNode = Node("20", Map[String, Int]("30" -> 1, "60" -> 1, "70" ->1, "100" -> 3))
    val intersectionSet = Set("30", "100")

    val unifiedNode = Node.unifyNodes(aNode, bNode, intersectionSet)

    unifiedNode.followers.size should equal(6)
    unifiedNode.followers("30") should equal(3)
    unifiedNode.followers("100") should equal(6)
    unifiedNode.followers("40") should equal(1)
    unifiedNode.followers("50") should equal(1)
    unifiedNode.followers("60") should equal(1)
    unifiedNode.followers("70") should equal(1)
  }
}
