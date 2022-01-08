package cn.ioceye.streaming.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

/**
 * 业务：统计最近1分钟的数据,然后统计出热门商品
 * 应用slide window函数
 * 
 * nohup hive --service metastore > metastore.log 2>&1 &
 */
object StreamTop3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("spark://master:7077")
                        .setAppName(StreamTop3.getClass.getSimpleName)
    val streamContext = new StreamingContext(sparkConf, Durations.seconds(10))
    
     // 日志格式：category product
    val productLogDStream    = streamContext.socketTextStream("master", 9999);
    val productWordDStream   = productLogDStream.map(productLog => {
      val productWords       = productLog.split(" ")
      (productWords(0) + "_" + productWords(1), 1)
    })
    /**
     * 分析：batch的时间为10s,windowDuration=60s,也就是RDD数量=windowDuration/batchDuration=60/10=6,
     *      slideDuration会拿取之前windowDuration的数据,进行reduceByKey操作。
     * 脚下留心：slideDuration必须为batchDuration的整数倍
     */
    val productWordCountDStream = productWordDStream.reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Durations.seconds(60), Durations.seconds(60), 3)
    
    productWordDStream.foreachRDD(rdd => {
      val categoryProductCountRowRDD = rdd.map(product => {
        val productSplit = product._1.split("_")
        Row(productSplit(0), productSplit(1), product._2)
      })
    	val structFileds = List(
    			StructField("category", StringType, true),
    			StructField("product", StringType, true),
    			StructField("click_count", IntegerType, true)
    			)
			val structType = StructType(structFileds)
			
			val hiveContext           = new HiveContext(categoryProductCountRowRDD.context)
			val categoryProductDFrame = hiveContext.createDataFrame(categoryProductCountRowRDD, structType)
			
			categoryProductDFrame.write.mode(SaveMode.Append).saveAsTable("stream_hive")
      
      val sql = s"""
                | select category, product, count
                | from  (
                |        select category,
                |               product,
                |               count,
                |               row_number() over(partition by category order by count desc) as rank
                |        from (
                |                select category, product, sum(click_count) as count from stream_hive group by category, product
                |              ) t
                |       ) temp
                | where rank <= 3
                """.stripMargin('|')
      val top3DataFrame = hiveContext.sql(sql)
      
      top3DataFrame.show()
    })
    
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true)
  }
}