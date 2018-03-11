import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object Preprocessor {

  def readTextFile(filename: String, spark: SparkSession): Dataset[Node] ={
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

  def getUsersFromPairs(idPairs: Dataset[(String, String)], spark: SparkSession): Dataset[Node] ={
    import spark.implicits._

    idPairs.groupByKey(_._1)
      .mapGroups{ case (key, values) =>
        val tempMap = new mutable.HashMap[String, Int]
        values.foreach{ case (_, value) =>
          if (tempMap.contains(value)){
            tempMap.put(value, tempMap(value)+1)
          }
          else {
            tempMap.put(value, 1)
          }
        }
        Node(key, tempMap.toMap)
      }
  }

}
