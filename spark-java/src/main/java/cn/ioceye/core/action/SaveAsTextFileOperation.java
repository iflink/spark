package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SaveAsTextFileOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("SaveAsTextFileOperation");
				  
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList);
		
		JavaRDD<Integer> doubleRDD = numberRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value) throws Exception {
				return value * 2;
			}
		});
		
		// 直接将RDD中的数据保存在文件中
		// 注意：这里指定的为目录
		doubleRDD.saveAsTextFile("hdfs://master:9000/user/spark/tmp/double-number-java");
		
	}
}
