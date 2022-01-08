package cn.ioceye.df;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * nohup hive --service metastore > metastore.log 2>&1 &
 * 
 * spark-submit \
 * 	--class cn.ioceye.df.HiveDataFrame \
 * 	--num-executors 3 \
 * 	--driver-memory 500m \
 * 	--executor-memory 500m \
 * 	--executor-cores 3 \
 * 	spark-java-1.0.0.jar
 * @author adolf felix
 */
//脚下留心：只支持HiveContext，并且只能提交在集群上运行
public class HiveDataFrame {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().
				setMaster("spark://192.168.1.20:7077").
				setAppName(HiveDataFrame.class.getSimpleName());
		
		JavaSparkContext context = new JavaSparkContext(conf);
		HiveContext hiveContext = new HiveContext(context);
		
		hiveContext.sql("drop table if exists student_info");
	    hiveContext.sql("create table if not exists student_info(name string, age int) row format delimited fields terminated by ',' stored as textfile");
	    hiveContext.sql("load data local inpath 'data/df/student_info.txt' into table student_info");
	    
	    
	    hiveContext.sql("drop table if exists student_score");
	    hiveContext.sql("create table if not exists student_score(name string, score int) row format delimited fields terminated by ',' stored as textfile");
	    hiveContext.sql("load data local inpath 'data/df/student_score.txt' into table student_score");
	    
	    DataFrame studentDF = hiveContext.sql("select si.name, si.age, ss.score from student_info si left join student_score ss on si.name=ss.name");
	    studentDF.show();
	    
	    hiveContext.sql("drop table if exists student_info_score");
	    studentDF.write().saveAsTable("student_info_score");
	    
	    DataFrame studentFromHiveDF = hiveContext.table("student_info_score");
	    studentFromHiveDF.show();
	    
	    context.close();
	}
}
