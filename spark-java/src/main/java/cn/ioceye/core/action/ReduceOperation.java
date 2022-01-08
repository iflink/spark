package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("ReduceOperation")
				      .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList);
		int result = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println("reduce result = " + result);
	}
}
