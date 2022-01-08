package cn.ioceye.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Properties
import org.apache.spark.sql.SaveMode
import scala.collection.mutable.Map

object JdbcDataFrame {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                    .setMaster("local")
                    .setAppName(JdbcDataFrame.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    
    val loadOption = Map(
        "driver"->"com.mysql.jdbc.Driver", 
        "url"->"jdbc:mysql://master:3306/test",
        "dbtable"-> "student_info",
        "user"-> "hive",
        "password"-> "2011180062",
        "fetchsize"-> "3000"
    )
    val studentInfoDF = sqlContext.read.format("jdbc").options(loadOption).load()
    studentInfoDF.registerTempTable("student_info")
    studentInfoDF.printSchema()
    studentInfoDF.show()
    
    val saveProperties = new Properties()
    val url = "jdbc:mysql://master:3306/test"
		val dbTable = "yong_student_info"
		saveProperties.put("driver", "com.mysql.jdbc.Driver")
		saveProperties.put("user", "hive")
		saveProperties.put("password", "2011180062")
		saveProperties.put("fetchsize", "3000")
		
		val yongStudentInfoDF = sqlContext.sql("select name,age,info from student_info")
		yongStudentInfoDF.write.mode(SaveMode.Overwrite).format("jdbc").jdbc(url, dbTable, saveProperties);
    
    loadOption("dbtable") = dbTable
    val loadLongStudentInfoDF = sqlContext.read.format("jdbc").options(loadOption).load()
    loadLongStudentInfoDF.printSchema()
    loadLongStudentInfoDF.show()
  }
}