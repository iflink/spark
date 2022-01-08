package cn.ioceye.core.started;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

@SuppressWarnings("resource")
public class RDDPersist {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("RDDPersist")
				      .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/**
		 * cache或者persist使用是由规则的
		 * 必须在transformation或者textFile等创建了一个RDD后,直接连续调用cache或persist才可以。
		 * 如果你先创建RDD,然后单独另起一行执行cache或persist是没有用的,并且会报错,大量的文件会丢失。
		 */
		// 第一步
		// JavaRDD<String> lineRDD = jsc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 3);
		// 第二步
		JavaRDD<String> lineRDD = jsc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 3).cache();
		// 持久化策略
		// JavaRDD<String> lineRDD = jsc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 3)
		//                              .persist(StorageLevel.DISK_ONLY());
		
		long beginTime = System.currentTimeMillis();
		long count = lineRDD.count();
		long endTime = System.currentTimeMillis();
		System.out.println("count = " + count + ", cost time =" + (endTime - beginTime) + " ms");
		
		// no persist
		beginTime = System.currentTimeMillis();
		count = lineRDD.count();
		endTime = System.currentTimeMillis();
		System.out.println("count = " + count + ", cost time =" + (endTime - beginTime) + " ms");
		
	}
}
