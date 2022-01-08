package cn.ioceye.df.func

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
              setMaster("local").
              setAppName(DailyUV.getClass.getSimpleName())
    
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    
    // 模拟用户访问日志, 日期:用户ID
    val userAccessLog = Array(
        "2018-10-01:1122", 
        "2018-10-01:1122",
        "2018-10-01:1123",
        "2018-10-01:1124",
        "2018-10-01:1124",
        "2018-10-02:1121",
        "2018-10-02:1122",
        "2018-10-02:1123",
        "2018-10-02:1123"
    )
    val userAccessLogRDD = sparkContext.parallelize(userAccessLog, 3);
    val userAccesssLogRowRDD = userAccessLogRDD.map(line => {
      val words = line.split(":")
      Row(words(0), words(1).toInt)
    })
    
    val structFileds = Array(
        StructField("date", StringType, true),
        StructField("user_id", IntegerType, true)
    )
    val structType = StructType(structFileds)
    
    val userAccessLogDF = sqlContext.createDataFrame(userAccesssLogRowRDD, structType)
    userAccessLogDF.show()
    
    // 内置函数:countDistinct
    /**
     * 业务：对每天用户访问量进行统计(UserVistor -> UV)
     */
    // 导入spark sql的内置函数
    import org.apache.spark.sql.functions._
    // 如果需要使用spark sql的内置函数,必须导入SQLContext的隐式转换
    import sqlContext.implicits._
    val aggDF = userAccessLogDF.groupBy("date")
                               .agg('date, countDistinct('user_id))
   aggDF.show()
   aggDF.map(row => Row(row(0), row(2)))
        .collect()
        .foreach(println)
  }
}