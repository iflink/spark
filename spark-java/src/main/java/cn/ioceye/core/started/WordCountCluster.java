package cn.ioceye.core.started;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * WordCount Test For Spark Cluster
 * @author adolf.felix
 */
@SuppressWarnings("resource")
public class WordCountCluster {
	public static void main(String[] args) {
		/**
		 * 如果要在Spark集群上运行,需要修改的地方,只有2个
		 * 1.将SparkConf的setMaster方法删掉,默认她自己会去链接
		 * 2.针对hadoop hdfs上真正存储的大数据文件
		 * 
		 * 执行步骤：
		 * 1.将wordcount.txt文件上传到hdfs
		 * 2.使用pom.xml配置的maven插件,对spark工程打包
		 * 3.将打包后的spark工程jar包,上传到机器上执行
		 * 4.编写spark-submit脚本
		 * 5.执行spark-submit脚本,提交spark应用到集群
		 */
		SparkConf conf = new SparkConf()
							 .setAppName("WordCountCluster");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String line) throws Exception {
				line = line.replaceAll("[,.]", "");
				return Arrays.asList(line.split(" "));
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + "=" + wordCount._2);
			}
		});
	}
}
