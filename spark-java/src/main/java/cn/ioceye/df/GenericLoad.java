package cn.ioceye.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class GenericLoad {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			     			.setAppName(GenericLoad.class.getSimpleName())
			     			.setMaster("local");
		
		JavaSparkContext context    = new JavaSparkContext(conf);
		SQLContext       sqlContext = new SQLContext(context);
		
		DataFrame dataFrame = sqlContext
							  .read()
				              .load("hdfs://master:9000/user/spark/data/df/student.parquet/part-r-00000-18fe8265-7eb8-4a65-b785-9255219a86fa.gz.parquet");
		
		dataFrame.printSchema();
		dataFrame.select("id", "name", "age").show();
	}
}
