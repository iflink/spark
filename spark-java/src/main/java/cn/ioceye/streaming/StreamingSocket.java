package cn.ioceye.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * StreamingContext开发的步骤：
 * 1. 创建输入DStream接入输入源。
 * 2. 对DStream执行transformation和output算子操作,实现计算逻辑。
 * 3. 调用StreamingContext的start方法,启动实时处理数据。
 * 4. 调用StreamingContext的awaitTermination方法,等待应用终止。
 *    也可以使用CTRL + C停止,或者让它持续不断的运行进行计算。
 * 5. 也可以通过调用StreamingContext的stop方法,来停止应用程序。
 * 
 * 需要注意的点：
 * 1. 只要一个StreamingContext启动之后,就不能再在后面执行其他任何算子。
 *    比如：执行start之后,不能为DStream执行任何算子。
 * 2. StreamingContext stop之后,不能再次调用start重启。
 * 3. 一个JVM同时只能有一个StreamingContext启动。也就是：在一个应用程序中,不能同时启动2个StreamingContext。
 * 4. StreamingContext调用stop方法之后,会同时停止内部的SparkContext,如果不停止StreamingContext,可以调用stop(false)。
 * 5. 一个SparkContext可以创建多个StreamingContext,只要调用stop(false),再创建一个StreamingContext即可。
 * 
 * yum install -y nc
 * nc -lk 9999
 * 脚下留心：安装的nc最好用光驱的安装包,以免运行出错。
 * @author lolog
 */
public class StreamingSocket {
	public static void main(String[] args) {
		// local[2]: 表示用2个线程执行Spark Streaming程序
		SparkConf sparkConf = new SparkConf()
				                  .setMaster("local[2]")
				                  .setAppName(StreamingSocket.class.getSimpleName());
		/**
		 * batchDuration：表示每收集多长时间的数据,划分为1个batch进行处理。
		 *                可以根据应用程序的延迟要求,以及可用的集群资源情况来设置。
		 */
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		/**
		 * 首先必须创建输入DStream,代表不断从数据源(比如kafka、socket)接收的实时数据流。
		 * 调用JavaStreamingContext的socketTextStream方法,可以创建一个socket网络端口的数据流,JavaReceiverInputDStream代表一个DStream。
		 * 
		 * 脚下留心:DStream代表batchDuration间隔的数据流
		 */
		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("master", 9999);
		
		JavaDStream<String>     words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairDStream<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairDStream<String, Integer> wordCounts = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer count_1, Integer count_2) throws Exception {
				return count_1 + count_2;
			}
		});
		
		/**
		 * 注意：SparkStream的计算模型决定了,不断拉取batchDuration间隔的数据,数据不能保存在实例变量中,
		 *      需要实现缓存将每个batch的数据写入缓存持久化操作。
		 */
		wordCounts.print();
		
		/**
		 * start:            启动JavaStreamingContext
		 * awaitTermination: 等待结束
		 * close:            关闭连接
		 */
		streamingContext.start();
		streamingContext.awaitTermination();
		streamingContext.close();
	}
}
