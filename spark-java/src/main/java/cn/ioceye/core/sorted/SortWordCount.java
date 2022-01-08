package cn.ioceye.core.sorted;

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

public class SortWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				         .setAppName("SortWordCount")
				         .setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> linesRDD = jsc.textFile("hdfs://master:9000/user/spark/data/wordcount.txt", 3);
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				line = line.replaceAll("[,.]", "");
				return Arrays.asList(line.split(" "));
			}
		});
		JavaPairRDD<String, Integer> wordPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairRDD<String, Integer> wordReduceByKeyRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		JavaPairRDD<Integer, String> wordMapToPairRDD = wordReduceByKeyRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> wordCount) throws Exception {
				return new Tuple2<Integer, String>(wordCount._2, wordCount._1);
			}
		});
		JavaPairRDD<Integer, String> wordSortByKeyRDD = wordMapToPairRDD.sortByKey();
		JavaPairRDD<String, Integer> wordResultRDD = wordSortByKeyRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> word) throws Exception {
				return new Tuple2<String, Integer>(word._2, word._1);
			}
		});
		
		wordResultRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> wordTuple) throws Exception {
				System.out.println(wordTuple._1 + " = " + wordTuple._2);
			}
		});
		jsc.close();
	}
}
