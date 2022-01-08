package cn.ioceye.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SaveMode

object GenericSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
			     			.setAppName(GenericSave.getClass.getSimpleName())
			     			.setMaster("local")
		
		val context    = new JavaSparkContext(conf)
		val sqlContext = new SQLContext(context)
		
		val dataFrame = sqlContext.read.json("hdfs://master:9000/user/spark/data/students.json")
		
		// 指定保存文件的格式
		// dataFrame.select("id", "name", "age").write.format("parquet").save("hdfs://master:9000/user/spark/data/df/student.parquet")
		
		/**
		 * mode
		 * 1.SaveMode.ErrorIfExists
		 * 2.SaveMode.Append
		 * 3.SaveMode.Overwrite
		 * 4.SaveMode.Ignore
		 */
		dataFrame.select("id", "name", "age").write.mode(SaveMode.Overwrite).save("hdfs://master:9000/user/spark/data/df/student.parquet")
  }
}