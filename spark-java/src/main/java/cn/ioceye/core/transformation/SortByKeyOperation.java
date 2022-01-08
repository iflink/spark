package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

@SuppressWarnings("resource")
public class SortByKeyOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		          conf.setAppName("GroupByOperation")
		              .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> scoreList = Arrays.asList(
				new Tuple2<Integer, String>(50, "leo"),
				new Tuple2<Integer, String>(23, "jary"),
				new Tuple2<Integer, String>(90, "felix"), 
				new Tuple2<Integer, String>(30, "jack")
		);
		
		JavaPairRDD<Integer, String> scoreRDD = jsc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer, String> sortByKeyRDD = scoreRDD.sortByKey(false);
		
		sortByKeyRDD.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> val) throws Exception {
				System.out.println(val._1 + " = " + val._2);
			}
		});
	}
}
