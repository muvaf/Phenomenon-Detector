import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, Matchers}

class NodeTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the true intersection set") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq.empty, "40" -> Seq.empty, "50" -> Seq.empty, "100" -> Seq.empty))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq.empty, "60" -> Seq.empty, "70" -> Seq.empty, "100" -> Seq.empty))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(2)
    intersectionSet should contain("30")
    intersectionSet should contain("100")
  }

  test("Returns empty node if there is no intersection") {
    val aNode = Node("10", Map[String, Seq[String]]("150" -> Seq.empty, "40" -> Seq.empty, "50" -> Seq.empty, "110" -> Seq.empty))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq.empty, "60" -> Seq.empty, "70" -> Seq.empty, "100" -> Seq.empty))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(0)
  }

  test("Returns unified node") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq.empty, "40" -> Seq.empty, "50" -> Seq.empty, "100" -> Seq.empty))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq.empty, "60" -> Seq.empty, "70" -> Seq.empty, "100" -> Seq.empty))
    val intersectionSet = Set("30", "100")

    val unifiedNode = Node.unifyNodes(aNode, bNode, intersectionSet)

    unifiedNode.followers.size should equal(6)
    unifiedNode.followers("30") should contain("20")
    unifiedNode.followers("100") should contain("20")

    unifiedNode.followers("30").size should equal(1)
    unifiedNode.followers("100").size should equal(1)
    unifiedNode.followers("40").size should equal(0)
    unifiedNode.followers("50").size should equal(0)
    unifiedNode.followers("60").size should equal(0)
    unifiedNode.followers("70").size should equal(0)
  }

  test("Returns unified node with existing intersection") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq("40"), "40" -> Seq.empty, "50" -> Seq.empty, "100" -> Seq.empty))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq.empty, "60" -> Seq.empty, "70" -> Seq.empty, "100" -> Seq.empty))
    val intersectionSet = Set("30", "100")

    val unifiedNode = Node.unifyNodes(aNode, bNode, intersectionSet)

    unifiedNode.followers.size should equal(6)
    unifiedNode.followers("30") should contain("20")
    unifiedNode.followers("30") should contain("40")
    unifiedNode.followers("100") should contain("20")

    unifiedNode.followers("30").size should equal(2)
    unifiedNode.followers("100").size should equal(1)
    unifiedNode.followers("40").size should equal(0)
    unifiedNode.followers("50").size should equal(0)
    unifiedNode.followers("60").size should equal(0)
    unifiedNode.followers("70").size should equal(0)
  }

}
