import org.scalatest.{FunSuite, Matchers}

class DiscoveryEngineTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the merger score") {
    val aNode = Node("10", Map(
      "30" -> Seq("20"),
      "40" -> Seq.empty,
      "50" -> Seq("60", "70", "80"),
      "70" -> Seq.empty
    ))
    val bNode = Node("100", Map(
      "30" -> Seq.empty,
      "60" -> Seq.empty,
      "80" -> Seq.empty
    ))

    val score = DiscoveryEngine.getMergerScore(aNode, bNode)

    score._2 should equal(7.2)
  }

}