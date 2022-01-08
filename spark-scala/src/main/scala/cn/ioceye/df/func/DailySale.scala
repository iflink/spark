package cn.ioceye.df.func

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object DailySale {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
              setMaster("local").
              setAppName(DailyUV.getClass.getSimpleName())
    
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    
    // 模拟用户访问日志, 日期:用户ID:销售量
    val userAccessLog = Array(
        "2018-10-01:1122:55.06", 
        "2018-10-01:1122:60.32",
        "2018-10-01:1133:66.21",
        "2018-10-01:1133:110.24",
        "2018-10-01:1156:78.25",
        "2018-10-02:1123:90.02",
        "2018-10-02:1123:89.03",
        "2018-10-02:2256:90.22",
        "2018-10-02:1156:80.45"
    )
    val userAccessLogRDD = sparkContext.parallelize(userAccessLog, 3);
    val userAccesssLogRowRDD = userAccessLogRDD.map(line => {
      val words = line.split(":")
      Row(words(0), words(1).toInt, words(2).toDouble)
    })
    
    val structFileds = Array(
        StructField("date", StringType, true),
        StructField("user_id", IntegerType, true),
        StructField("sale", DoubleType, true)
    )
    val structType = StructType(structFileds)
    
    val userAccessLogDF = sqlContext.createDataFrame(userAccesssLogRowRDD, structType)
    userAccessLogDF.show()
    
    // 内置函数:countDistinct
    /**
     * 业务：统计用户每天的销量
     */
    // 导入spark sql的内置函数
    import org.apache.spark.sql.functions._
    val aggDF = userAccessLogDF.groupBy("date", "user_id")
                               .agg(sum("sale").alias("sum"), max("sale").alias("max"), min("sale").alias("min"))
    aggDF.show()
  }
}