package cn.ioceye.core.advance

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("Top3")
               .setMaster("local")
    val sc = new SparkContext(conf)
    
    val lineRDD = sc.textFile("hdfs://master:9000/user/spark/data/sort.txt", 3)
    val numberRDD = lineRDD.flatMap(line => {
      val wordList = ArrayBuffer[Int]()
      for(word <- line.split(" ")) {
        wordList += word.toInt
      }
      wordList
    })
    val numberPairRDD = numberRDD.map(number => (number, 1))
    val sortedNumberRDD = numberPairRDD.sortByKey(false, 3)
    val tackNumberList  = sortedNumberRDD.take(3)
    
    for(tackNumber <- tackNumberList) {
      println("number = " + tackNumber._1)
    }
  }
}