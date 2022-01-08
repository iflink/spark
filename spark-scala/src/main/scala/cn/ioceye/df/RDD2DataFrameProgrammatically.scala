package cn.ioceye.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import cn.ioceye.df.pojo.Student
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.RowFactory
import scala.collection.immutable.List
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

object RDD2DataFrameProgrammatically {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
                    .setAppName(RDD2DataFrameProgrammatically.getClass.getSimpleName)
                    .setMaster("local")
    val context     = new SparkContext(conf)
    val sqlContext  = new SQLContext(context)
    
    val lineRDD = context.textFile("hdfs://master:9000/user/spark/data/students.json", 3)
    
    val studentRDD = lineRDD.map(line => {
      val js   = JSON.parseObject(line)
      
      val id   = js.getInteger("id")
      val name = js.getString("name")
      val age  = js.getInteger("age")
      
      RowFactory.create(id, name, age)
    })
    
    // 编程方式动态构造元数据
    val fieldTypes = List(DataTypes.createStructField("id", DataTypes.IntegerType, true),
                          DataTypes.createStructField("name", DataTypes.StringType, true),
                          DataTypes.createStructField("age", DataTypes.IntegerType, true))
    
    // RDD转换为DataFrame
    val structType = StructType(fieldTypes)
    
    val studentDF = sqlContext.createDataFrame(studentRDD, structType)
                          
    studentDF.registerTempTable("students")
    
    /**
     * 调用SqlContext.sql方法, 用SqlParser生成一个Unresolved LogicalPlan,
     * 并且封装在DataFrame中。
     */
    val sqlStudentDF = sqlContext.sql("select id, name, age from students")
    /**
     * 调用DataFrame打印操作,或者rdd的transaformation、action操作,就会触发SQLContext
     * 的executeSql操作,executeSql调用执行QueryExecution的操作。
     * 
     * 然后,调用Analyzer的apply方法,使用Unresolved LogicalPlan生成Resolved LogicalPlan,
     * 也就是将LogicalPlan与SQL语句的数据源执行绑定。
     */
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
}