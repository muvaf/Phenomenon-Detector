import org.apache.spark.sql.Row
import org.scalatest.{FunSuite, Matchers}

class NodeTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the true intersection set") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq("10"), "40" -> Seq("10"), "50" -> Seq("10"), "100" -> Seq("10")))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq("20"), "60" -> Seq("60"), "20" -> Seq("20"), "100" -> Seq("20")))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(2)
    intersectionSet should contain("30")
    intersectionSet should contain("100")
  }

  test("Returns empty node if there is no intersection") {
    val aNode = Node("10", Map[String, Seq[String]]("150" -> Seq("10"), "40" -> Seq("10"), "50" -> Seq("10"), "110" -> Seq("10")))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq("20"), "60" -> Seq("20"), "70" -> Seq("20"), "100" -> Seq("20")))

    val intersectionSet = Node.getIntersectionSet(aNode, bNode)

    intersectionSet.size should equal(0)
  }

  test("Returns unified node") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq("10"), "40" -> Seq("10"), "50" -> Seq("10"), "100" -> Seq("10")))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq("20"), "60" -> Seq("20"), "70" -> Seq("20"), "100" -> Seq("20")))
    val intersectionSet = Set("30", "100")

    val unifiedNode = Node.unifyNodes(aNode, bNode, intersectionSet)

    unifiedNode.followers.size should equal(6)
    unifiedNode.followers("30") should contain("20")
    unifiedNode.followers("30") should contain("30")
    unifiedNode.followers("100") should contain("20")
    unifiedNode.followers("100") should contain("100")

    unifiedNode.followers("30").size should equal(2)
    unifiedNode.followers("100").size should equal(2)
    unifiedNode.followers("40").size should equal(1)
    unifiedNode.followers("50").size should equal(1)
    unifiedNode.followers("60").size should equal(1)
    unifiedNode.followers("70").size should equal(1)
  }

  test("Returns unified node with existing intersection") {
    val aNode = Node("10", Map[String, Seq[String]]("30" -> Seq("30", "40"), "40" -> Seq("40"), "50" -> Seq("50"), "100" -> Seq("100")))
    val bNode = Node("20", Map[String, Seq[String]]("30" -> Seq("30"), "60" -> Seq("60"), "70" -> Seq("70"), "100" -> Seq("100")))
    val intersectionSet = Set("30", "100")

    val unifiedNode = Node.unifyNodes(aNode, bNode, intersectionSet)

    unifiedNode.followers.size should equal(6)
    unifiedNode.followers("30") should contain("20")
    unifiedNode.followers("30") should contain("40")
    unifiedNode.followers("30") should contain("30")
    unifiedNode.followers("100") should contain("20")
    unifiedNode.followers("100") should contain("100")

    unifiedNode.followers("30").size should equal(3)
    unifiedNode.followers("100").size should equal(2)
    unifiedNode.followers("40").size should equal(1)
    unifiedNode.followers("50").size should equal(1)
    unifiedNode.followers("60").size should equal(1)
    unifiedNode.followers("70").size should equal(1)
  }

}
