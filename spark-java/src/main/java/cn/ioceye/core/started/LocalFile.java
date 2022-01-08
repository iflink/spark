package cn.ioceye.core.started;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

@SuppressWarnings("resource")
public class LocalFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
							 .setMaster("local")
							 .setAppName("LocalFile");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// 使用SparkContext以及其子类的textFile方法,针对本地文件创建RDD
		JavaRDD<String> lines = jsc.textFile("data/wordcount.txt");
		
		// 统计文本内的字数
		JavaRDD<Character> charactorRDD = lines.flatMap(new FlatMapFunction<String, Character>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Character> call(String line) throws Exception {
				List<Character> charactors = new ArrayList<Character>();
				for(int index=0; index<line.length(); index++) {
					if((line.charAt(index) >= 'a' && line.charAt(index) <= 'z')
					   || (line.charAt(index) >= 'A' && line.charAt(index) <= 'Z')) {
						charactors.add(line.charAt(index));
					}
				}
				return charactors;
			}
		});
		JavaPairRDD<Character, Integer> charactorPair = charactorRDD.mapToPair(new PairFunction<Character, Character, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Character, Integer> call(Character charactor) throws Exception {
				return new Tuple2<Character, Integer>(charactor, 1);
			}
		});
		JavaPairRDD<Character, Integer> charactorCounts = charactorPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		charactorCounts.foreach(new VoidFunction<Tuple2<Character,Integer>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<Character, Integer> charactorCount) throws Exception {
				System.out.println(charactorCount._1 + "=" + charactorCount._2);
			}
		});
		
		// 关闭
		//jsc.close();
	}
}
