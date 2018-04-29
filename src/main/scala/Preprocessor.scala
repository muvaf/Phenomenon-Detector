import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Preprocessor {

  def readTextFile(filename: String, sc: SparkContext): RDD[Node] ={
    val idPairs = getDatasetFromText(filename, sc)
    getUsersFromPairs(idPairs, sc)
  }

  def getDatasetFromText(filename: String, sc: SparkContext): RDD[(String, String)] ={
    val rawData = sc.textFile(filename)

    // Revert the ordering as the data is in "a follows b" format but we want "b followed by a"
    rawData.map(value => value.split(" "))
      .map{ pairArray =>
        (pairArray(1), pairArray(0))
      }
  }

  def getUsersFromPairs(idPairs: RDD[(String, String)], sc: SparkContext): RDD[Node] ={

    idPairs.groupByKey()
      .map { case (key, values) =>
        val tempMap = mutable.Map.empty[String, Int]
        values.foreach{ value =>
          if (tempMap.contains(value)){
            tempMap.put(value, tempMap(value)+1)
          }
          else {
            tempMap.put(value, 0)
          }
        }
        Node(key, tempMap.toMap)
      }
  }

}
