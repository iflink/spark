package cn.ioceye.core.started

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCountCluster {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("WordCountCluster")
               
    val sc = new SparkContext(conf)
    sc.addJar(args(0));
    
    val lines = sc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 1)
    
    val words = lines.flatMap(line => line.replaceAll("[,.]", "").split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey((v1, v2) => v1 + v2)
    
    wordCounts.foreach(wordCount => println(wordCount._1 + "=" + wordCount._2))
  }
}