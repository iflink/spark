package cn.ioceye.core.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * collect:将远程分布式的RDD聚合到本地
 * @author adolf felix
 *
 */
public class CollectOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("CollectOperation")
				      .setMaster("local");
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
		
		/**
		 * 不用foreach action操作,在远程集群上变量RDD的元素
		 * 而是使用collect操作,将分布在远程集群上的RDD数据拉到本地
		 * 
		 * 脚下留心：这种方式一般不建议使用,因为如果RDD中的数据量比较大的话,通过网络传输到本地,性能会很差。
		 * 此外,除了性能差,还可能在RDD数据量特别大的情况下,发生OOM异常,内存溢出。因此通过推荐使用foreach
		 * action操作,对远程数据执行输出。
		 */
		List<Integer> doubleLocalRDD = doubleRDD.collect();
		for(Integer number: doubleLocalRDD) {
			System.out.println("number = " + number);
		}
	}
}
