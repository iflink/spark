package cn.ioceye.core.sorted

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("SortWordCount")
               .setMaster("local")
    val sc = new SparkContext(conf)
    
    val lineRDD = sc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 3)
    
    val wordRDD = lineRDD.flatMap(line => {
      val _line = line.replaceAll("[,.]", "")
      _line.split(" ")
    })
    val wordPairRDD         = wordRDD.map(word => (word, 1))
    val wordCountRDD        = wordPairRDD.reduceByKey((v1, v2) => v1 + v2)
    val reverseWordCountRDD = wordCountRDD.map(wordCount => (wordCount._2, wordCount._1))
    val sortWordRDD         = reverseWordCountRDD.sortByKey(true, 3)
    val wordSortCountRDD    = sortWordRDD.map(sortWord => (sortWord._2, sortWord._1))
    
    wordSortCountRDD.foreach(wordSorCount => println(wordSorCount._1 + " = " + wordSorCount._2))
    
    sc.stop
  }
}