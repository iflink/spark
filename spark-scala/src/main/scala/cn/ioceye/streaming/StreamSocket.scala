package cn.ioceye.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext

object StreamSocket {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamSocket.getClass.getSimpleName)
    // 方式1
    val streamingContext = new StreamingContext(sparkConf, Duration(10 * 1000))
    /**
     * 方式2
     * val sparkContext      = new SparkContext(sparkConf)
     * val streamingContext_ = new StreamingContext(sparkContext, Durations.seconds(10))
     */
    
    val lineDStream = streamingContext.socketTextStream("master", 9999)
    val wordDStream = lineDStream.flatMap(line => {
      val words = line.split(" ")
      words.toList
    })
    val wordTupleDStream = wordDStream.map(word => (word, 1))
    val resultDStream = wordTupleDStream.reduceByKey((v1, v2) => v1 + v2)
    resultDStream.print()
    
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}