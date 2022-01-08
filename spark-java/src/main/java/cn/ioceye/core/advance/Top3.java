package cn.ioceye.core.advance;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Top3 {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				         .setAppName("Top3")
				         .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lineRDD = jsc.textFile("hdfs://master:9000/user/spark/data/sort.txt", 3);
		
		JavaRDD<Integer> numberRDD = lineRDD.flatMap(new FlatMapFunction<String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Integer> call(String line) throws Exception {
				List<Integer> numberList = new ArrayList<>();
				for(String word: line.split(" ")) {
					numberList.add(Integer.valueOf(word));
				}
				return numberList;
			}
		});
		JavaPairRDD<Integer, Integer> numberPairRDD = numberRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Integer> call(Integer number) throws Exception {
				return new Tuple2<Integer, Integer>(number, 1);
			}
		});
		JavaPairRDD<Integer, Integer> sortedNumberRDD = numberPairRDD.sortByKey(false);
		JavaRDD<Integer> numberMapRDD = sortedNumberRDD.map(new Function<Tuple2<Integer,Integer>, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Tuple2<Integer, Integer> sortedNumber) throws Exception {
				return sortedNumber._1;
			}
		});
		List<Integer> tackNumberList = numberMapRDD.take(3);
		for(Integer number: tackNumberList) {
			System.out.println("number = " + number);
		}
		jsc.close();
	}
}
