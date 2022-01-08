package cn.ioceye.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import cn.ioceye.exporter.MysqlPool

/**
 * create table if not exists stream_kafka (
 *    keyword varchar(50),
 *    total int
 * );
 * 
 * kafka-topics.sh --create --zookeeper master:2181,slave1:2181,slave2:2181 --topic
 *                stream_kafka --replication-factor 1 --partitions 2
 * kafka-console-producer.sh --broker-list master:2181,slave1:2181,slave2:2181
 *                --topic stream_kafka
 * kafka-console-producer.sh --broker-list master:9092 --topic stream_kafka
 * kafka-console-consumer.sh --zookeeper master:2181,slave1:2181,slave2:2181 --from-beginning --topic stream_kafka
 */
object StreamKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName(StreamKafkaReceiver.getClass.getSimpleName)
    val sc = new StreamingContext(sparkConf, Durations.seconds(30))
    
    val topicThreadMap = Map("stream_kafka" -> 1)
    val lineDStream = KafkaUtils.createStream(sc, 
                                              "master:2181,slave1:2181,slave2:2181", 
                                              "default", 
                                              topicThreadMap);
    val wordDStream = lineDStream.flatMap(line => {
      val words = line._2.split(" ")
      words
    })
    
    val wordTupleDStream = wordDStream.map(word => {
      (word, 1)
    })
    
    val reduceWordDStream = wordTupleDStream.reduceByKey((v1, v2) => v1 + v2)
    
    // save to mysql
    reduceWordDStream.foreachRDD(rdd => {
      rdd.foreachPartition(records => {
    	  MysqlPool.saveRDD("stream_kafka", records)
      })
    })
    
    sc.start()
    sc.awaitTermination()
    sc.stop(true)
  }
}