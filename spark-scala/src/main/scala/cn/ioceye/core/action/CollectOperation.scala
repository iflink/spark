package cn.ioceye.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CollectOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("CollectOperation")
            
    val sc = new SparkContext(conf)
    val numberArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    
    val numberRDD = sc.parallelize(numberArr, 3);
    
    val localRDD = numberRDD.collect()
    
    for(collect <- localRDD) {
      println("collect = " + collect)
    }
  }
}