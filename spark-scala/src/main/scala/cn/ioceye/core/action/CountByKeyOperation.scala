package cn.ioceye.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountByKeyOperation {
  def main(args: Array[String]): Unit = {
    def conf = new SparkConf()
               .setMaster("local")
               .setAppName("CountByKeyOperation")
               
    def sc = new SparkContext(conf)
        
    def scores = Array(("class1", 10), ("class2", 20), ("class1", 20))
    val scoreRDD = sc.parallelize(scores, 3)
    
    val scoreCountRDD = scoreRDD.countByKey()
    
    scoreCountRDD.foreach(g => {
        println(g._1 + " = " + g._2) 
      })
  }
}