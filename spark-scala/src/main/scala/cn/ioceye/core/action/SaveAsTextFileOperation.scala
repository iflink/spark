package cn.ioceye.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SaveAsTextFileOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("SaveAsTextFileOperation")
               
    val sc = new SparkContext(conf)
    val numberArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    
    val numberRDD = sc.parallelize(numberArr, 3);
    
    val mapRDD = numberRDD.map(m => m * 2)
    
    mapRDD.saveAsTextFile("hdfs://master:9000/user/spark/tmp/double-number-scala")
  }
}