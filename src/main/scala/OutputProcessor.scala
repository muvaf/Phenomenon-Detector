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

  def writeToFile(seedNode: Node, intersectionAmounts: Set[(Seq[String], Int)]): Unit ={
    val pw = new PrintWriter(new File("output.txt" ))
    val sumOfIntersections = seedNode.followers.values.map(_.size).sum - seedNode.followers.size
    val summary = "nodeNum:"+(seedNode.subNodes.size+1).toString+
      ";coverage:"+seedNode.followers.size+
      ";sumOfIntersections:"+sumOfIntersections+
      ";subNodes:"+seedNode.subNodes.reduce((a, b) => a + "," + b)
    pw.println(summary)
    println(summary)
    intersectionAmounts.foreach{ case (group, amount) =>
      val quotedGroup = group.map("\'" + _ + "\',").reduce(_+_)
      val fixLastComma = quotedGroup.substring(0, quotedGroup.size-1)
      val groupString = "[" + fixLastComma + "]"
      pw.println(groupString + "=" + amount.toString)
      println(groupString + "=" + amount.toString)
    }
    pw.close()
  }

}
