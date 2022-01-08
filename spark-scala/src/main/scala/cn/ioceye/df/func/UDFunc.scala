package cn.ioceye.df.func

import scala.reflect.api.materializeTypeTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object UDFunc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
              setMaster("local").
              setAppName(UDFunc.getClass.getSimpleName())

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    
    val names = Array("Leo", "Marray", "Jack", "Tom")
    
    val nameRDD    = sparkContext.parallelize(names, 3)
    val nameRowRDD = nameRDD.map(name => Row(name))
    
    val structType = StructType(Array(StructField("name", StringType, true)))
    val nameDF     = sqlContext.createDataFrame(nameRowRDD, structType)
    nameDF.registerTempTable("name_table")
    
    // 注册自定义函数
    sqlContext.udf.register("str_len", (str: String) => str.length());
    
    val queryNameDF = sqlContext.sql("select name, str_len(name) as len from name_table")
    queryNameDF.printSchema()
    queryNameDF.show()
    
  }
}