package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

@SuppressWarnings("resource")
public class ColgroupOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		          conf.setAppName("GroupByOperation")
		              .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "jack"),
				new Tuple2<Integer, String>(1, "mary"),
				new Tuple2<Integer, String>(2, "jack")
		);
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 10101),
				new Tuple2<Integer, Integer>(2, 10102),
				new Tuple2<Integer, Integer>(2, 10103)
		);
		
		// 并行化2个集合
		JavaPairRDD<Integer, String> studentRDD = jsc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scoreRDD = jsc.parallelizePairs(scoreList);
		
		/**
		 * colgroup: key对应的值已经分组
		 * key -> ((v1_0,v1_2), (v2_0,v2_1))
		 * 
		 * join    : key对应的值没有分组
		 * key  -> (v1_0, v2_0)
		 * key  -> (v1_1, v2_1)
		 */
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = studentRDD.cogroup(scoreRDD);
		cogroupRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tv) throws Exception {
				System.out.println("+++++++++++++++++++");
				System.out.println(tv._1);
				System.out.println(tv._2._1);
				System.out.println(tv._2._2);
			}
		});
	}
}
