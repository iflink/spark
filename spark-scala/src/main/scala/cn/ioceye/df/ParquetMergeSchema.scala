package cn.ioceye.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SaveMode

object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
			     			.setAppName(ParquetMergeSchema.getClass.getSimpleName())
			     			.setMaster("local")
		
		val context    = new JavaSparkContext(conf)
		val sqlContext = new SQLContext(context)
    
    import sqlContext.implicits._
    
    val studentInfo = List(("leo", 23), ("jack", 25))
    val studentInfoDF = context.parallelize(studentInfo, 2).toDF("name", "age")
    studentInfoDF.write.format("parquet").mode(SaveMode.Append).save("hdfs://master:9000/user/spark/data/df/studentinfo")
   
    val studentGrade = List(("marry", "A"), ("tom", "B"))
    val studentGradeDF = context.parallelize(studentGrade, 2).toDF("name", "grade")
    studentGradeDF.write.format("parquet").mode(SaveMode.Append).save("hdfs://master:9000/user/spark/data/df/studentinfo")
    
    // 2个DataFrame结构是不相同的, 期望自动合并为3个列:name,age,grade
    // 使用mergeSchema方式,读取studentinfo中的数据,进行元数据的合并
    val studentDF = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://master:9000/user/spark/data/df/studentinfo")
    studentDF.printSchema()
    studentDF.show()
  }
}