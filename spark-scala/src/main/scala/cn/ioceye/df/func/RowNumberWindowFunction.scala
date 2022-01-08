package cn.ioceye.df.func

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.hive.HiveContext


// 脚下留心：只支持HiveContext
object RowNumberWindowFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
              setMaster("spark://master:7077").
              setAppName(RowNumberWindowFunction.getClass.getSimpleName())
    
    val sparkContext = new SparkContext(conf)
    val hiveContext  = new HiveContext(sparkContext)
    
    hiveContext.sql("drop table if exists phone_sale")
    hiveContext.sql("create table if not exists phone_sale (date string, user_id string, sale_name string, sale int) row format delimited fields terminated by ',' stored as textfile")
    hiveContext.sql("load data local inpath 'data/df/phone-sales.txt' into table phone_sale")
    /**
     * 业务：统计用户每天的销量的Top3
     * 
     * row_number开窗函数
     * 1.select查询可以使用row_number函数
     * 2.row_number函数后面先跟上over关键字。然后,partition by是分组的字段。其次,order by是可以根据字段进行排序
     * 3.row_number的结果为：分组之后的组内行号
     */
    val sql = """ 
                 select 
                   date,
                   user_id,
                   sale,
                   rk
                 from (
                   select 
                     date,
                     user_id,
                     sale,
                     row_number() over (partition by date,user_id order by sale desc) as rk 
                   from 
                     phone_sale
                 ) tmp_phone_sales 
                 where rk < 3
              """
    val phoneSaleRankDF = hiveContext.sql(sql)
    phoneSaleRankDF.show()
  }
}