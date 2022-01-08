package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

@SuppressWarnings("resource")
public class FilterOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("FilterOperation")
				      .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);
		
		JavaRDD<Integer> filterNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer number) throws Exception {
				return (number % 2 == 0);
			}
		});
		
		filterNumberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer number) throws Exception {
				System.out.println(number);
			}
		});
	}
}
