import java.io.{File, PrintWriter}

import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

object OutputProcessor {

  def getListOfPairs(node: Node, mainDatabase: Dataset[Node], spark: SparkSession): Set[(Node, Node)] ={
    val inclusionList = Set(node.id) ++ node.subNodes
    val chosenNodes = mainDatabase.filter{ node =>
      inclusionList.contains(node.id)
    }
    val rowList = chosenNodes.crossJoin(chosenNodes)
      .filter{ row =>
      !row.get(0).equals(row.get(3))
      }.collect()

    Set.empty ++ rowList.map(Node.getNodePairFromRow(_, spark))
  }

  def getIntersectionAmount(pairs: Set[(Node, Node)]): Set[((Node, Node), Int)] ={
    pairs.map{ case (aNode, bNode) =>
        val intersectionSet = Node.getIntersectionSet(aNode, bNode)
      ((aNode, bNode), intersectionSet.size)
    }
  }

  def writeToFile(seedNode: Node, intersectionAmounts: Set[((Node, Node), Int)]): Unit ={
    val pw = new PrintWriter(new File("output.txt" ))
    pw.println("nodeNum:"+(seedNode.subNodes.size+1).toString+
      ";coverage:"+seedNode.followers.size+
      ";sumOfIntersections:"+seedNode.followers.values.sum+
      ";subNodes:"+seedNode.subNodes.reduce((a, b) => a + "," + b))
    intersectionAmounts.foreach{ pair =>
      pw.println(pair._1._1.id + "," + pair._1._2.id + ":" + pair._2)
    }
    pw.close()
  }

}
