package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * count: 统计元素个数
 * @author adolf felix
 *
 */
public class CountByKeyOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("CollectOperation")
				      .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> users = Arrays.asList(
				new Tuple2<String, Integer>("jack", 1),
				new Tuple2<String, Integer>("jack", 2),
				new Tuple2<String, Integer>("jack", 3),
				new Tuple2<String, Integer>("jack", 4),
				new Tuple2<String, Integer>("felix", 1)
		);
		
		JavaPairRDD<String, Integer> userRDD = jsc.parallelizePairs(users);
		
		Map<String, Object> keyCounts = userRDD.countByKey();
		
		System.out.println(keyCounts);
	}
}
