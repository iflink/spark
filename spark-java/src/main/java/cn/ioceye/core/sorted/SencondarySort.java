package cn.ioceye.core.sorted;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SencondarySort {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				         .setAppName("SencondarySort")
				         .setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> linesRDD = jsc.textFile("hdfs://master:9000/user/spark/data/sort.txt", 3);
		JavaPairRDD<SencondarySortKey, String> linePairRDD = linesRDD.mapToPair(new PairFunction<String, SencondarySortKey, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SencondarySortKey, String> call(String line) throws Exception {
				String[] words = line.split(" ");
				SencondarySortKey key = new SencondarySortKey(Integer.valueOf(words[0]), Integer.valueOf(words[1]));
				return new Tuple2<SencondarySortKey, String>(key, line);
			}
		});
		JavaPairRDD<SencondarySortKey, String> lineSortRDD = linePairRDD.sortByKey();
		lineSortRDD.foreach(new VoidFunction<Tuple2<SencondarySortKey,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<SencondarySortKey, String> sortedKeys) throws Exception {
				System.out.println(sortedKeys._1.getFirst() + " " + sortedKeys._1.getSecond());
			}
		});
		jsc.close();
	}
}
