import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object Preprocessor {

  def readTextFile(filename: String, spark: SparkSession): Dataset[User] ={
    val idPairs = getDatasetFromText(filename, spark)
    getUsersFromPairs(idPairs, spark)
  }

  def getDatasetFromText(filename: String, spark: SparkSession): Dataset[(String, String)] ={
    import spark.implicits._

    val rawData = spark.read.text(filename).as[String]

    // Revert the ordering as the data is in "a follows b" format but we want "b followed by a"
    rawData.map(value => value.split(" "))
      .map{ pairArray =>
        (pairArray(1), pairArray(0))
      }
  }

  def getUsersFromPairs(idPairs: Dataset[(String, String)], spark: SparkSession): Dataset[User] ={
    import spark.implicits._

    idPairs.groupByKey(_._1)
      .mapGroups{ case (key, values) =>
        val newNode = User(key, new mutable.HashMap[String, Int])
        values.foreach{ case (_, value) =>
          if (newNode.followingList.contains(value)){
            newNode.followingList.put(value, newNode.followingList(value)+1)
          }
          else {
            newNode.followingList.put(value, 1)
          }
        }
        newNode
      }
  }

}
