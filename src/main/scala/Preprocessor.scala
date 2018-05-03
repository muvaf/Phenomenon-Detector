import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

object Preprocessor {

  def readTextFile(filename: String, sc: SparkContext): RDD[Node] ={
    val idPairs = getDatasetFromText(filename, sc)
    getNodesFromPairs(idPairs, sc)
  }

  def getDatasetFromText(filename: String, sc: SparkContext): RDD[(String, String)] ={
    val rawData = sc.textFile(filename)

    // Revert the ordering as the data is in "a follows b" format but we want "b followed by a"
    rawData.map(value => value.split(" "))
      .map{ pairArray =>
        (pairArray(1), pairArray(0))
      }
  }

  def getNodesFromPairs(idPairs: RDD[(String, String)], sc: SparkContext): RDD[Node] ={

    idPairs.groupByKey()
      .map { case (key, values) =>
        val tempMap = mutable.Map.empty[String, Seq[String]]
        values.foreach{ value =>
          // There might be duplicate entries in the data. So, we're handling them here.
          tempMap.put(value, Seq.empty)
        }
        Node(key, tempMap.toMap)
      }
  }

}
