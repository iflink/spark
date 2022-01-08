package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object JoinOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("FlatMapOperation")
        
    val sc = new SparkContext(conf)
        
    val users = Array((1, "jack"), (2, "kafka"), (3, "tom"), (4, "leo"), (4, "foko"))
    val roles = Array((1, 1), (2, 3), (3, 3), (4, 4), (4, 41))
    
    val userRDD = sc.parallelize(users, 3)
    val roleRDD = sc.parallelize(roles, 3)
    
    val colgroupRDD = userRDD.cogroup(roleRDD)
    colgroupRDD.foreach(g => {
      println(g._1)
      println(g._2._1)
      println(g._2._2)
    })
  }
}