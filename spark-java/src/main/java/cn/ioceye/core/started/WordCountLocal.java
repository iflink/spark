package cn.ioceye.core.started;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Local WordCount Test
 * @author adolf.felix
 */
@SuppressWarnings("resource")
public class WordCountLocal {
	public static void main(String[] args) {
		/**
		 * +++++++++++ 1.创建SparkConf对象,设置Spark应用的配置信息 +++++++++++
		 */
		SparkConf conf = new SparkConf()
							 // 设置Spark App名字,以便状态监控
							 .setAppName("WorldCountLocal")
							 // setMaster: 设置需要连接Spark集群master节点的url;如果设置为local,则代表本地运行。
							 .setMaster("local");
		/** 
		 * +++++++++++ 2.创建JavaSparkContext对象 +++++++++++
		 * 在Spark中,SparkContext是Spark所有功能的一个入口,无论使用Java、Scala、Python都需要使用SparkContext,
		 *  > 主要作用是：包括初始化Spark应用程序所需要的一些核心组件、调度器(DAGScheduler、TaskScheduler),以及注册应用
		 *  > 到Spark Master节点上等等。
		 * 一言以概之, SparkContext是Spark应用中,可以说是最重要的一个对象。 
		 * 但是呢,在Spark中,编写不同类型的Spark应用程序,使用的SparkContext时不同的;
		 *  > Scala使用的是原生的SparkContext对象
		 *  > Java使用的是JavaSparkContext对象
		 *  > 如果开发Spark SQL程序,使用的是SQLContext、HiveContext
		 *  > 如果开发Spark Streaming程序,使用的就是它独有的SparkContext
		 */
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/**
		 * +++++++++++ 3.针对输入源(hdfs文件、本地文件等等),创建初始的RDD. +++++++++++
		 *  > 输入源的数据会被打散分配到每个RDD中,从而形成一个初始的分布式数据集。
		 *  > 我们这里因为是针对本地测试,所以就是针对本地文件。
		 * 
		 * SparkContext中,用于根据文件类型的输入源创建RDD,叫做textFile方法。
		 * 在Java中,创建的普通RDD,都叫做JavaRDD。
		 * 
		 * 在这里,RDD中有元素这个概念,如果HDFS或者本地文件创建的RDD,每一个元素就相当于文件里的一行。
		 */
		JavaRDD<String> lines = jsc.textFile("data/wordcount.txt");
		
		/**
		 * +++++++++++ 4.对初始RDD进行transformation操作,也就是一些初始化操作 +++++++++++
		 *  > 通常操作会通过创建function,并且配合RDD的map、flatMap等算子来执行function。通常,如果
		 *  > 比较简单,则创建指定Funtion的匿名内部类。但是,如果function比较复杂,则会单独创建一个类,作为
		 *  > 这个funtion借口的类
		 */
		// 先将每一行拆分为单个单词
		// FlatMapFunction, 有2个泛型参数,分别代表输入和输出类型。
		// 在这里,输入肯定是string类型,因为是一行一行的文本; 
		// 输出其实也是string,因为是每一行的文本拆分的单词。
		// 这里先介绍flatMap算子的作用,其实就是将RDD的一个元素给拆分成一个或多个元素。
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String line) throws Exception {
				line = line.replaceAll("[,.]", "");
				return Arrays.asList(line.split(" "));
			}
		});
		// 接着,需要将每个单词映射为(word, 1)的这种格式。
		// 因为只有这样,后面才能根据单词作为key,来进行每个单词出现次数的累积。
		// mapToPair：就是将每个元素,映射为一个(v1,v2)这样的Tuple类型的元素。
		//           要求的是与PairFunction配合使用
		//           第一个参数代表输入类型, 第二个代表输出Tuple2的v1, 第二个参数代表输出Tuple2的v2
		// JavaPairRDD的2个泛型参数,分别代表tuple元素的第一个和第二份值的类型。
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		// 接着,需要以单词作为key,统计每个单词出现的次数
		// 这里要使用reduceByKey这个算子,对每个key对应的value,都进行reduce操作。
		// reduce操作,相当于把第一个值和第二个值进行计算,然后再将结果与第三个值进行计算。
		//  > 比如,JavaPairRDD中有几个元素,(hello, 1) (hello, 1) (hello, 1) (hello, 1)
		//  > 对于key=hello, 1+1=2,然后再进行2+1=3...
		//  > 返回的结果：返回的JavaPairRDD中的元素,也是Tuple,第一个值为key,第二个值是key的value。
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		// 到此为止,通过几个Spark算子操作,以及统计出了单词的次数。
		// 但是,之前使用的flatMap、reduceByKey这种操作,都叫做transformation操作。
		// 一个spark应用中,光是transformation操作是不行的,是不会执行的,必须有一个叫做action才能执行操作。
		// 接着,最后可以使用一种action操作,比如说foreach操作来触发程序的执行。
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + "=" + wordCount._2);
			}
		});
	}
}
