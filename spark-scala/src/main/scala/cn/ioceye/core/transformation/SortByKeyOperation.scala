package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortByKeyOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("FlatMapOperation")
        
    val sc = new SparkContext(conf)
        
    val scores = Array((60, "jack"), (50, "kafka"), (90, "tom"), (89, "leo"))
    val scoreRDD = sc.parallelize(scores, 3)
    
    val scoreGroupRDD = scoreRDD.sortByKey(false, 3)
    
    scoreGroupRDD.foreach(g => {
        println(g._1 + " = " + g._2) 
      })
  }
}