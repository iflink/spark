package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

@SuppressWarnings("resource")
public class GroupByKeyOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		          conf.setAppName("GroupByOperation")
		              .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 10),
				new Tuple2<String, Integer>("class2", 20), 
				new Tuple2<String, Integer>("class1", 20)
		);
		
		JavaPairRDD<String, Integer> scoreRDD = jsc.parallelizePairs(scoreList);
		
		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = scoreRDD.groupByKey();
		
		groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> val) throws Exception {
				System.out.println(val._1);
				for(Integer score: val._2) {
					System.out.println(score);
				}
			}
		});
	}
}
