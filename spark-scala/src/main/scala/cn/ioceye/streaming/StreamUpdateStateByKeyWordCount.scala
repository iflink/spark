package cn.ioceye.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

import cn.ioceye.exporter.MysqlPool

// 基于DStream updateStateByKey实现缓存机制的实时wordcount
object StreamUpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamUpdateStateByKeyWordCount.getClass.getSimpleName)
    val sparkContext     = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Durations.seconds(20))
    
    val prevStartTimeAccumulator = sparkContext.accumulator[Long](0, "prevStartTime")
    
    val startTime = System.currentTimeMillis()
    prevStartTimeAccumulator.setValue(startTime)
    
    /**
     * 使用updateStateByKey算子,必须设置一个checkpoint目录,开启checkpoint机制。
     * 那么,key对应的state会保存一份在内存中,还会保存一份到checkpoint目录。
     * 优点：如果内存数据丢失,那么就会从checkpoint中恢复数据。
     */
    // 开启checkpoint机制
    streamingContext.checkpoint("hdfs://master:9000/user/spark/data/checkpoint")
    
    val lineDStream = streamingContext.socketTextStream("master", 9999)
    val wordDStream = lineDStream.flatMap(line => {
      val words = line.split(" ")
      words.toList
    })
    val wordTupleDStream = wordDStream.map(word => (word, 1))
    /**
     * 每个单词每次batch计算都会调用该lambda函数。
     * currentValues      : batch中,该key的一系列新值
     * previousValueState : 为该key之前的状态,泛型的类型为自己指定的
     */
    val wordCountDStream = wordTupleDStream.updateStateByKey((newValues: Seq[Int], runningCount: Option[Int]) => {
      val currentCount  = newValues.sum
      val previousCount = runningCount.getOrElse(0)
      
      Some(currentCount + previousCount)
    })
    
    wordCountDStream.print(100)
    
    wordCountDStream.foreachRDD(rdd => {
      if((System.currentTimeMillis() - prevStartTimeAccumulator.value) > 60 * 1000){
        rdd.foreachPartition(records => {
          MysqlPool.saveRDD("stream_hdfs", records)
        })
        
        prevStartTimeAccumulator.setValue(System.currentTimeMillis())
      }
    })
      
    
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true)
  }
}