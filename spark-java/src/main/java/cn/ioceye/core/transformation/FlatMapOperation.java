package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

@SuppressWarnings("resource")
public class FlatMapOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("FlatMapOperation")
				      .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<String> numbers = Arrays.asList("hello word", "welcome to here");
		JavaRDD<String> numberRDD = jsc.parallelize(numbers);
		
		JavaRDD<String> flatRDD = numberRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		flatRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});
	}
}
