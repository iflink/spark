package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GroupByKeyOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("FlatMapOperation")
        
    val sc = new SparkContext(conf)
        
    val scores = Array(("class1", 10), ("class2", 20), ("class1", 20))
    val scoreRDD = sc.parallelize(scores, 3)
    
    val scoreGroupRDD = scoreRDD.groupByKey()
    
    scoreGroupRDD.foreach(g => {
        println("class = " + g._1) 
        g._2.foreach(score => println(score))
      })
  }
}