package cn.ioceye.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext

import cn.ioceye.exporter.MysqlPool

/**
 * create table if not exists stream_hdfs (
 *    keyword varchar(50),
 *    total int
 * )
 */
object StreamHDFS {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamHDFS.getClass.getSimpleName)
                        
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(10))
    
    // 测试：将文件上传到/user/spark/data/stream文件夹下
    val lineDStream = streamingContext.textFileStream("hdfs://master:9000/user/spark/data/stream")
    val wordDStream  = lineDStream.flatMap(line => {
      val words = line.split(" ")
      words.toList
    })
    val wordTupleDStream  = wordDStream.map(word => (word, 1))
    val reduceWordDStream = wordTupleDStream.reduceByKey((v1, v2) => v1 + v2)
    
    // save to mysql
    reduceWordDStream.foreachRDD(rdd => {
      rdd.foreachPartition(records => {
    	  MysqlPool.saveRDD("stream_hdfs", records)
      })
    })
    
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true)
  }
}