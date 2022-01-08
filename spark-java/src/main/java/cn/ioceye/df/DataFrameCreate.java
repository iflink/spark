package cn.ioceye.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DatFrame
 * @author felix
 */
public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
						     .setAppName("DataFrameCreate")
						     .setMaster("local");
		JavaSparkContext context    = new JavaSparkContext(conf);
		SQLContext       sqlContext = new SQLContext(context);
		
		
		DataFrame  df = sqlContext.read().json("hdfs://master:9000/user/spark/data/students.json");
		
		// 打印DataFrame中的所有数据
	    // 等价于：select * from ...
	    df.show();
	    
	    // 打印DataFrame的元数据(Schema)
	    df.printSchema();
	    
	    // 查询某列所有的数据
	    // select name from ...
	    df.select("name").show();
	    
	    // 查询多列数据,并对列进行计算
	    df.select(df.col("name"), df.col("age").plus(1)).show();
	    
	    // 对某一列的值进行过滤
	    df.filter(df.col("age").gt(18)).show();
	    
	    // 根据某一列进行分组,然后进行聚合
	    df.groupBy(df.col("name")).count().show();
	}
}
