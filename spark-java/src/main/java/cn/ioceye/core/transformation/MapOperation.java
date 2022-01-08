package cn.ioceye.core.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

@SuppressWarnings("resource")
public class MapOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("MapOperation")
				      .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numbers);
		
		// 使用map算子,将集合中的每个元素都乘以2
		// map算子,是对任何类型的RDD,都可以调用的
		// 在java中,map算子接收的参数是Function对象
		// 创建的Function对象,一定会让你设置第二个泛型参数,这个泛型类型,就是返回的新元素的类型。
		// 同时调用call方法的返回类型,也必须与第二个泛型类型同步
		// 在call方法内部,就可以对原始RDD中的每个元素进行各种处理和计算,并返回一个新的元素,所有新的元素就会组成一个新的RDD
		JavaRDD<Integer> multiNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer number) throws Exception {
				return number * 2;
			}
		});
		
		multiNumberRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer number) throws Exception {
				System.out.println(number);
			}
		});
	}
}
