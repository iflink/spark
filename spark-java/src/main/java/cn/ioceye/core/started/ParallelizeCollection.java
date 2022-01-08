package cn.ioceye.core.started;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 并行化集合创建RDD
 * @author adolf felix
 */
@SuppressWarnings("resource")
public class ParallelizeCollection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
							 .setAppName("ParallelizeCollection")
							 .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		// 要通过并行化集合方式创建RDD,需要调用SparkContext壹基金其子诶的parallelize方法
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);
		
		// 执行reduce算子操作
		Integer result = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		System.out.println("result = " + result);
	}
}
