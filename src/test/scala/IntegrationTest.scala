import org.scalatest.{FunSuite, Matchers}

class IntegrationTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Gets the true merged node and score") {
    val allNodes = Preprocessor.readTextFile("src/test/samples/integdata.txt", sc).collect()
    val aNode = allNodes.filter(_.id == "a").head
    val bNode = allNodes.filter(_.id == "b").head
    val cNode = allNodes.filter(_.id == "c").head

    val abNodes = Node.mergeNodes(aNode, bNode)
    val abcNodes = Node.mergeNodes(abNodes, cNode)

    val followers = abcNodes.followers
    followers("f").toSet should contain("a")
    followers("h").toSet should contain("a")

    followers("z").toSet should contain("a")
    followers("z").toSet should contain("c")

    followers("p").toSet should contain("a")
    followers("p").toSet should contain("c")

    followers("k").toSet should contain("a")
    followers("k").toSet should contain("b")
    followers("k").toSet should contain("c")

    followers("m").toSet should contain("a")
    followers("m").toSet should contain("b")
    followers("m").toSet should contain("c")

    followers("d").toSet should contain("b")
    followers("e").toSet should contain("b")

    followers("t").toSet should contain("c")
    followers("t").toSet should contain("b")
    followers("s").toSet should contain("c")
    followers("s").toSet should contain("b")

    followers("g").toSet should contain("c")
    followers("j").toSet should contain("c")

    val score = DiscoveryEngine.getScore(abcNodes)._2
    score should equal(7.2)

  }

}