package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

@SuppressWarnings("resource")
public class ReduceByKeyOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		          conf.setAppName("ReduceByKeyOperation")
		              .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 10),
				new Tuple2<String, Integer>("class2", 20), 
				new Tuple2<String, Integer>("class1", 20)
		);
		
		JavaPairRDD<String, Integer> scoreRDD = jsc.parallelizePairs(scoreList);
				
		JavaPairRDD<String, Integer> reduceRDD = scoreRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		reduceRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> record) throws Exception {
				System.out.println(record._1 + " = " + record._2);
			}
		});
	}
}
