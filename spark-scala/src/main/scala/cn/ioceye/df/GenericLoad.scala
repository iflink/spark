package cn.ioceye.df

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.api.java.JavaSparkContext

object GenericLoad {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
			     			.setAppName(GenericLoad.getClass.getSimpleName())
			     			.setMaster("local");
		
		val context    = new JavaSparkContext(conf);
		val sqlContext = new SQLContext(context);
		
		// 指定load数据源的格式
		// sqlContext.read.format("json").load("hdfs://master:9000/user/spark/data/students.json")
		val dataFrame = sqlContext
							      .read
				            .load("hdfs://master:9000/user/spark/data/df/student.parquet/part-r-00000-18fe8265-7eb8-4a65-b785-9255219a86fa.gz.parquet");
		
		dataFrame.printSchema();
		dataFrame.select("id", "name", "age").show();
  }
}