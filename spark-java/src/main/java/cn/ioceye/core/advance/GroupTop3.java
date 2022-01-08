package cn.ioceye.core.advance;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupTop3 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		         .setAppName("Top3")
		         .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lineRDD = jsc.textFile("hdfs://master:9000/user/spark/data/groupsort.txt", 3);
		JavaPairRDD<String, Integer> wordPairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				String[] words = line.split(" ");
				return new Tuple2<String, Integer>(words[0], Integer.valueOf(words[1]));
			}
		});
		JavaPairRDD<String, Iterable<Integer>> groupByClassRDD = wordPairRDD.groupByKey();
		JavaPairRDD<String, Iterable<Integer>> classTop3RDD = groupByClassRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> clazz) throws Exception {
				List<Integer> scores = new ArrayList<>();
				for(Integer score: clazz._2) {
					scores.add(score);
				}
				scores.sort(new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return -(o1 - o2);
					}
				});
				List<Integer> top3 = new ArrayList<>();
				int size = scores.size() > 3 ? 3 : scores.size();
				for(int i=0; i<size; i++) {
					top3.add(scores.get(i));
				}
				return new Tuple2<String, Iterable<Integer>>(clazz._1, top3);
			}
		});
		classTop3RDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> clazz) throws Exception {
				System.out.println(clazz._1 + " = " + clazz._2);
			}
		});
	}
}
