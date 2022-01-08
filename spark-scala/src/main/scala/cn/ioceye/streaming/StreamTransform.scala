package cn.ioceye.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations

/**
 * 业务：广告计费日志实时黑名单过滤
 * 用户对广告进行点击,根据点击次数进行计费,但是,有不良商家
 * 出现刷广告的情况, 因此就需要设置一个黑名单对用户进行过滤。
 */
object StreamTransform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamTransform.getClass.getSimpleName)
    val streamContext = new StreamingContext(sparkConf, Durations.seconds(10))
    
    // 黑名单RDD
    val blackUsers           = List(("tom", true))
    val blackUserRDD         = streamContext.sparkContext.parallelize(blackUsers, 3);
    
    // 日志文本格式：date username
    val adClickLogDStream    = streamContext.socketTextStream("master", 9999);
    
    // (username, date username)
    val userAdClickDStream   = adClickLogDStream.map(log => {
      val logSplits = log.split(" ")
      (logSplits(1), log)
    })
    
    val userClickDStream = userAdClickDStream.transform(userAd => {
      /**
       * 为什么需要用左外链接呢？
       * 因为不是每个用户都存在于黑名单中的。
       */
      val joinedUserRDD   = userAd.leftOuterJoin(blackUserRDD)
      // 没有包含黑名单的用户
      val validateUserRDD = joinedUserRDD.filter(jionedUser => {
        if(jionedUser._2._2.isEmpty || jionedUser._2._2.get == false) {
          true
        }
        else {
          false
        }
      })
      
      // date username
      val adClickLogRDD = validateUserRDD.map(validateUser => {
        validateUser._2._1
      })
      
      adClickLogRDD
    })
    
    userClickDStream.print()
    
    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop(true)
  }
}