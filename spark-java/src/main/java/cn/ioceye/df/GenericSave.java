package cn.ioceye.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class GenericSave {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			     			.setAppName(GenericSave.class.getSimpleName())
			     			.setMaster("local");
		
		JavaSparkContext context    = new JavaSparkContext(conf);
		SQLContext       sqlContext = new SQLContext(context);
		
		DataFrame dataFrame = sqlContext.read().json("hdfs://master:9000/user/spark/data/students.json");
		dataFrame.select("id", "name", "age").write().save("hdfs://master:9000/user/spark/data/df/student.parquet");
		
		dataFrame.show();
		dataFrame.printSchema();
	}
}
