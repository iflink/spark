package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * count: 统计元素个数
 * @author adolf felix
 *
 */
public class CountOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("CollectOperation")
				      .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList);
		
		Long count = numberRDD.count();
		System.out.println("count = " + count);
	}
}
