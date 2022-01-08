package cn.ioceye.df.func

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object UDAFunc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
                   setMaster("local").
                   setAppName(UDAFunc.getClass.getSimpleName())
    

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    
    val names = Array("Leo", "Marray", "Jack", "Tom", "Tom")
    
    val nameRDD    = sparkContext.parallelize(names, 3)
    val nameRowRDD = nameRDD.map(name => Row(name))
    
    val structType = StructType(Array(StructField("name", StringType, true)))
    val nameDF     = sqlContext.createDataFrame(nameRowRDD, structType)
    nameDF.registerTempTable("name_table")
    
    // 注册自定义函数
    sqlContext.udf.register("str_len", new Count);
    
    val queryNameDF = sqlContext.sql("select name, str_len(name) as len from name_table group by name")
    queryNameDF.printSchema()
    queryNameDF.show()
  }

  class Count extends UserDefinedAggregateFunction {
    // 输入数据的类型
    def inputSchema: StructType = {
      StructType(Array(StructField("str", StringType, true)))
    }
    // 中间进行聚合时,所处理的数据类型
    def bufferSchema: StructType = {
      StructType(Array(StructField("count", IntegerType, true)))
    }
    // 函数返回值的类型
    def dataType: DataType = {
       IntegerType
    }
    // 为每个group by的数据执行初始化操作
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
    }
    // 对于每个分组,如果接收到新值,如何对其进行进行聚合计算
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Int](0) + 1
    }
    def deterministic: Boolean = {
      true
    }
    /**
     * 由于spark是分布式的,因此一个分组的数据,可能存在于多个节点上,
     * 因此需要在不同的节点上进行聚合作用,就是update。但是,最后一个
     * 分组在各个节点上的聚合,要进行merge操作,也就是合并
     */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    }
    // 一个分组的聚合值,如何通过中间的缓存聚合值,最后返回一个最终的聚合值
    def evaluate(buffer: Row): Any = {
      buffer.getAs[Int](0)
    }
  }
}