import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import java.util.Random
import scala.math.exp

object Arm {
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Association Rule Mining")
    val sc = new SparkContext(sparkConf)

    // Read dataset file
    val minSupport = 2500
    
    val lines = sc.textFile("/home/rahul/rahul/rahul_misc/spark/ARM/dataset/chess.dat")
    val lineArrayRdd = lines.map(_.split(" ").map(_.toLong)).persist

    val transactionRdd = lines.flatMap {
      trans =>
        trans.split(" ").map(v => (v.toInt, 1))
    }
    val itemSet1 = transactionRdd.reduceByKey(_ + _).filter(_._2 > minSupport).collect.map(_._1.toInt).toList
    val itemSetList = itemSet1.map(v => List(v))
    println("First ds:"+itemSet1.toString)
    CountTree.addNewLevel(itemSetList)
    
    var cal = (!itemSet1.isEmpty)
    var cnt = 1
    
    while (cal) {
     println("Loop:"+cnt)
     val nextLevelDataS = CountTree.calcNextItemSet
     println("Dataset length:"+nextLevelDataS.size)
     val itemSet2 = lineArrayRdd.flatMap { //Remember to provide sorted list
        line =>
          val occurenceCountSet = nextLevelDataS.map {
            dataList =>
              val isPresent = dataList.foldLeft(true) {
                (a, b) =>
                  if (a == false) false
                  else if (line contains b) true
                  else false
              }
              if (isPresent) (dataList, 1)
              else (dataList, 0)
          }
          occurenceCountSet
      }
      val frequentItemSet = itemSet2.reduceByKey(_ + _).filter(_._2 > minSupport).collect.map(_._1).toList
      println("Calculated fset size:"+frequentItemSet.size)
      if(frequentItemSet.isEmpty) cal = false
      else {
        CountTree.addNewLevel(frequentItemSet)
      }
      cnt += 1
    }
    
    println("Writing to file.../tmp/freqItemFile.txt")
    CountTree.writeToFile("/tmp/freqItemFile.txt")
    sc.stop
  }
}


