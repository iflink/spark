package cn.ioceye.core.variable;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

@SuppressWarnings("resource")
public class AccumulatorVariable {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
						 .setMaster("local")
				         .setAppName("AccumulatorVariable");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList);
		
		Accumulator<Integer> accumulator = jsc.accumulator(0);
		
		numberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(Integer number) throws Exception {
				accumulator.add(number);
			}
		});
		
		// 只能在driver中获取其中,在action和transformation中不能访问值
		System.out.println("accumulator = " + accumulator.value());
	}
}
