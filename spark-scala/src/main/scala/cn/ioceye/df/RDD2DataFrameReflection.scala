package cn.ioceye.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.alibaba.fastjson.JSON
import cn.ioceye.df.pojo.Student

/**
 * 基于反射RDD到DataFrame转换, 
 * 必须用object extends App方式, 不能用def main运行程序, 否则会报no typetag for ... 错误
 */
object RDD2DataFrameReflection extends App{
    val conf = new SparkConf()
                 .setAppName("RDD2DataFrameReflection")
                 .setMaster("local")
    val context     = new SparkContext(conf)
    val sqlContext  = new SQLContext(context)
    
    val lineRDD = context.textFile("hdfs://master:9000/user/spark/data/students.json", 3)
    
    val studentRDD = lineRDD.map(line => {
      val js   = JSON.parseObject(line)
      
      val id   = js.getInteger("id")
      val name = js.getString("name")
      val age  = js.getInteger("age")
      
      Student(id, name, age)
    })
    
    // 隐式转换
    import sqlContext.implicits._
    val studentDF = studentRDD.toDF()
    
    studentDF.registerTempTable("students")
    
    val sqlStudentDF = sqlContext.sql("select id, name, age from students")
    sqlStudentDF.show()
    
    val studentResultRDD = sqlStudentDF.rdd.map(row => {
      val id   = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      val age  = row.getInt(2)
      
      Student(id, name, age)
    })
    
    val studentResultRDD2 = sqlStudentDF.rdd.map(row => {
      val rowMap = row.getValuesMap[Any](Array("id", "name", "age"))
      val id   = rowMap("id").toString().toInt
      val name = "_" + rowMap("name").toString()
      val age  = rowMap("age").toString().toInt
      
      Student(id, name, age)
    })
    
    studentResultRDD.foreach(println)
    studentResultRDD2.foreach(println)
}