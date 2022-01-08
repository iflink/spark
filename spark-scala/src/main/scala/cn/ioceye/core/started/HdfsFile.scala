package cn.ioceye.core.started

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object HdfsFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("HdfsFile")
               
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://master:9000//user/spark/data/wordcount.txt", 3)
    
    val charactorRDD = lines.flatMap(line => {
       val size = line.length
       val charactor = ArrayBuffer[Char]()
       for(i <- 0 until size if (line(i) >= 'a' && line(i) <= 'z' ) || (line(i) >= 'A' && line(i) <= 'Z')) {
         charactor += line(i)
       }
       charactor
    })
    val pairs = charactorRDD.map(charactor => (charactor, 1))
    val charactorCounts = pairs.reduceByKey(_ + _)
    
    charactorCounts.foreach(charactorCount => println(charactorCount._1 + "=" + charactorCount._2))
  }
}