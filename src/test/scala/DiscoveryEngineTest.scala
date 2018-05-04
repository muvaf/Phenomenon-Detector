import org.scalatest.{FunSuite, Matchers}

class DiscoveryEngineTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the merger score") {
    val aNode = Node("10", Map(
      "30" -> Seq("30","20"),
      "40" -> Seq("40"),
      "50" -> Seq("50","60", "70", "80"),
      "70" -> Seq("70")
    ))
    val bNode = Node("100", Map(
      "30" -> Seq("30"),
      "60" -> Seq("60"),
      "80" -> Seq("80")
    ))

    val score = DiscoveryEngine.getMergerScore(aNode, bNode)

    score._2 should equal(36.0/11.0)
  }

}