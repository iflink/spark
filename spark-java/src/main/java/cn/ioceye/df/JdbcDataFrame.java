package cn.ioceye.df;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class JdbcDataFrame {
	public static void main(String[] args) {
		Map<String, String> loadOptions = new HashMap<>();
		loadOptions.put("driver", "com.mysql.jdbc.Driver");
		loadOptions.put("url", "jdbc:mysql://master:3306/test");
		loadOptions.put("dbtable", "student_info");
		loadOptions.put("user", "hive");
		loadOptions.put("password", "2011180062");
		loadOptions.put("fetchsize", "3000");
		
		SparkConf sparkConf = new SparkConf()
								  .setMaster("local")
								  .setAppName(JdbcDataFrame.class.getSimpleName());
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext   sqlContext = new SQLContext(sparkContext);
		
		DataFrame studentInfoDF = sqlContext.read().format("jdbc").options(loadOptions).load();
		studentInfoDF.registerTempTable("student_info");
		studentInfoDF.printSchema();
		studentInfoDF.show();
		
		String url = "jdbc:mysql://master:3306/test";
		String dbTable = "yong_student_info";
		Properties properties = new Properties();
		properties.put("driver", "com.mysql.jdbc.Driver");
		properties.put("user", "hive");
		properties.put("password", "2011180062");
		properties.put("fetchsize", "3000");
		
		DataFrame yongStudentInfoDF = sqlContext.sql("select name, age, info from student_info");
		yongStudentInfoDF.write().mode(SaveMode.Append).format("jdbc").jdbc(url, dbTable, properties);
		
		loadOptions.put("dbtable", "yong_student_info");
		DataFrame loadLongStudentInfoDF = sqlContext.read().format("jdbc").options(loadOptions).load();
		loadLongStudentInfoDF.show();
		
		sparkContext.close();
	}
}
