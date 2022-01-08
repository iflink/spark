package cn.ioceye.core.variable;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量
 * @author adolf felix
 */
public class BroadcastVariable {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
				  conf.setAppName("BroadcastVariable")
				      .setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		final int factor = 3;
		final Broadcast<Integer> factorBroadcast = jsc.broadcast(factor);
		
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRDD = jsc.parallelize(numberList, 3);
		
		/**
		 * 让集合中的每个数字,都乘以外部定义的那个factor
		 * 广播变量的好处：避免执行的每个task都拷贝一份factor副本,导致网络和性能问题。
		 *            广播变量会将变量拷贝到节点,然后由节点分发共享到每个task。
		 */
		JavaRDD<Integer> doubleRDD = numberRDD.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				// 方式1:没有使用广播变量
				// return v1 * factor;
				
				// 方式2:使用广播变量
				Integer f = factorBroadcast.getValue();
				return v1 * f;
			}
		});
		
		doubleRDD.foreach(new VoidFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer number) throws Exception {
				System.out.println("number = " + number);
			}
		});
		jsc.close();
	}
}
