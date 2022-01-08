package cn.ioceye.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

/**
 * 业务：基于滑动滑动窗口搜索词实时统计
 */
object StreamSlideWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamSlideWindow.getClass.getSimpleName)
    val streamContext = new StreamingContext(sparkConf, Durations.seconds(10))
    
     // 搜索日志格式：username keyword
    val searchLogDStream    = streamContext.socketTextStream("master", 9999);
    // 提取搜索词
    val searchWordDStream   = searchLogDStream.map(searchLog => {
      val searchWords       = searchLog.split(" ")
      (searchWords(1), 1)
    })
    /**
     * 分析：batch的时间为10s,windowDuration=60s,也就是RDD数量=windowDuration/batchDuration=60/10=6,
     *      slideDuration会拿取之前windowDuration的数据,进行reduceByKey操作。
     * 脚下留心：slideDuration必须为batchDuration的整数倍
     */
    val searchWordCountDStreamRDD = searchWordDStream.reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Durations.seconds(60), Durations.seconds(20), 3)
    val searchWordTransformRDD    = searchWordCountDStreamRDD.transform((rdd, time) => {
      val countSearchWordRDD       = rdd.map(tuple => (tuple._2, tuple._1))
      // 执行降序排序
      val sortedCountSearchWordRDD = countSearchWordRDD.sortByKey(false, 3);
      val sortedSearchWordCountRDD = sortedCountSearchWordRDD.map(t => (t._2, t._1))
      
      // 取排名前三
      val top3SortedSearchWords  = sortedSearchWordCountRDD.take(3)
      
      // display top3
      top3SortedSearchWords.foreach(top3 => {
        println(top3._1 + ": " + top3._2)
      })
      
      sortedSearchWordCountRDD
    })
    
    searchWordTransformRDD.print()
    
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true)
  }
}