import org.scalatest.{FunSuite, Matchers}

class PreprocessorTest extends FunSuite
  with Matchers
  with SparkTestWrapper {

  test("Read id pairs text file, reverse order and give dataset"){
    val result = Preprocessor.getDatasetFromText("src/test/samples/sampledata.txt", sc).collect()
    result.length should equal(6)
    result should contain ("30", "50")
    result should contain ("60", "30")
    result should contain ("70", "60")
    result should contain ("70", "50")
    result should contain ("60", "50")
    result should contain ("70", "30")
  }

  test("Get dataset of User objects from id pairs of following connections"){
    val idPairs = sc.parallelize(Seq(("30", "50"), ("60", "30"), ("70", "60"), ("70", "50"), ("60", "50"), ("70", "30")))

    val rdd = Preprocessor.getUsersFromPairs(idPairs, sc)

    val result = rdd.collect().sortBy(_.id)

    result.length should equal(3)
    result(0).id should equal("30")
    result(0).followingList.keySet should contain("50")
    result(0).followingList("50") should equal(1)

    result(1).id should equal("60")
    result(1).followingList.keySet should contain("30")
    result(1).followingList.keySet should contain("50")
    result(1).followingList("30") should equal(1)
    result(1).followingList("50") should equal(1)

    result(2).id should equal("70")
    result(2).followingList.keySet should contain("30")
    result(2).followingList.keySet should contain("50")
    result(2).followingList.keySet should contain("60")
    result(2).followingList("30") should equal(1)
    result(2).followingList("50") should equal(1)
    result(2).followingList("60") should equal(1)

  }

}
