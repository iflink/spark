package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * task: 从远程获取前n个数据
 * @author adolf felix
 *
 */
public class TaskOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("CollectOperation")
				      .setMaster("local");
		@SuppressWarnings("resource")
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList);
		
		/**
		 * top    ：从分布在集群上获取n个RDD数据
		 * collect:获取全部数据
		 */
		List<Integer> top3Numbers = numberRDD.take(3);
		for(Integer top: top3Numbers) {
			System.out.println("top 3 = " + top);
		}
	}
}
