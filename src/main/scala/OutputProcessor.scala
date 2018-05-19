import java.io.{File, PrintWriter}

import scala.collection.mutable

object OutputProcessor {

  def getIntersectionAmounts(mainNode: Node): Set[(Seq[String], Int)] ={
    val intersectionMap = mutable.Map.empty[Set[String], Int]
    val onlyIntersectedFollowers = mainNode.followers.filter(_._2.nonEmpty)

    val intersectionGroups = onlyIntersectedFollowers.map(Set.empty[String] ++ _._2).toList
    intersectionGroups.foreach{ group =>
      if (intersectionMap.contains(group)){
        intersectionMap.put(group, intersectionMap(group) + 1)
      }
      else {
        intersectionMap.put(group, 1)
      }
    }
    intersectionMap.keySet.toSet.map{ key: Set[String] =>
      (key.toSeq, intersectionMap(key))
    }
  }

  def writeToFile(seedNode: Node, intersectionAmounts: Set[(Seq[String], Int)], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    val sumOfIntersections = seedNode.followers.values.map(_.size).sum - seedNode.followers.size
    val summary = "nodeNum:"+seedNode.subNodes.size.toString+
      ";coverage:"+seedNode.followers.size+
      ";sumOfIntersections:"+sumOfIntersections+
      ";subNodes:"+seedNode.subNodes.reduce((a, b) => a + "," + b)
    pw.println(summary)
    println(summary)
    pw.println("[")
    println("[")
    val sortedIntersectionAmounts = intersectionAmounts.toList.sortBy(_._1.size)
    sortedIntersectionAmounts.foreach{ case (group, amount) =>
      val quotedGroup = group.map("\'" + _ + "\',").reduce(_+_)
      val fixLastComma = quotedGroup.substring(0, quotedGroup.length-1)
      val groupString = "[" + fixLastComma + "]"
      val strToWrite =  "{ sets: " + groupString + ", size: " + amount.toString + " },"
      pw.println(strToWrite)
      println(strToWrite)
    }
    pw.println("];")
    println("];")
    pw.close()
  }

}
