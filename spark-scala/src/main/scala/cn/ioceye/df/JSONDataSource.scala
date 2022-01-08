package cn.ioceye.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
          		 .setMaster("local")
               .setAppName("DataFrameCreate")
               
    val sc         = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val studentScoreFileDF    = sqlContext.read.json("hdfs://master:9000/user/spark/data/student-score.json");
		studentScoreFileDF.registerTempTable("student_score")
		
		val highScoreStudentDF    = sqlContext.sql("select name, score from student_score where score >= 80")
		val highScoreStudentNames =  highScoreStudentDF.rdd.map(row => row.getString(0)).collect()
		
		val studentInfos   = Array[String]("{\"name\":\"jack\", \"age\":18}", "{\"name\":\"marry\", \"age\":17}", "{\"name\":\"leo\", \"age\":19}")
		val studentInfoRDD = sc.parallelize(studentInfos, 1)
		val studentInfoDF = sqlContext.read.json(studentInfoRDD)
		studentInfoDF.registerTempTable("student_info")
		
		var sql = "select name, age from student_info where name in ('" + highScoreStudentNames(0)
		for(index <- 1.until(highScoreStudentNames.length)) {
		  sql += "','" + highScoreStudentNames(index) 
		}
		sql += "')"
		
		val highScoreStudentInfoDF = sqlContext.sql(sql)
		
		val studentJoinRDD = highScoreStudentDF.map(row => {(row.getString(0), row.getLong(1).toInt)})
		.join(highScoreStudentInfoDF.map(row => (row.getString(0), row.getLong(1).toInt)))
		
		val studentRDD = studentJoinRDD.map(r => Row(r._1, r._2._1, r._2._2))
		
		val structFields = List(
		    DataTypes.createStructField("name", DataTypes.StringType, true),
		    DataTypes.createStructField("score", DataTypes.IntegerType, true),
		    DataTypes.createStructField("age", DataTypes.IntegerType, true)
		)
		
		val structType = StructType(structFields)
		
		val hightStudentRDD = sqlContext.createDataFrame(studentRDD, structType)
		
		hightStudentRDD.show()
  }
}