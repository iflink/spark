package cn.ioceye.df;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;

import cn.ioceye.df.pojo.Student;

/**
 * 使用编程方式动态指定元数据, 将RDD转换为DataFrame
 * 
 * @author felix
 */
public class RDD2DataFrameProgrammatically {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				         .setMaster("local")
				         .setAppName("RDD2DataFrameReflection");

		JavaSparkContext context    = new JavaSparkContext(conf);
		SQLContext       sqlContext = new SQLContext(context);

		JavaRDD<String>  lineRDD = context.textFile("hdfs://master:9000/user/spark/data/students.json");
		JavaRDD<Row>     rowRDD  = lineRDD.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String line) throws Exception {
				JSONObject json = JSONObject.parseObject(line);
				int    id   = json.getInteger("id");
				String name = json.getString("name");
				int    age  = json.getInteger("age");

				return RowFactory.create(id, name, age);
			}
		});

		/**
		 * 动态构造元数据
		 * 适合于从数据库加载数据, 动态创建
		 */
		List<StructField> fieldTypes = new ArrayList<>();
		fieldTypes.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fieldTypes.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fieldTypes.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(fieldTypes);
		
		// 使用动态构造的元数据,将RDD转换为DataFrame
		DataFrame studentDataFrame = sqlContext.createDataFrame(rowRDD, structType);

		// 拿到一个DataFrame之后,就可以将其注册为一个临时表,然后针对其中的数据执行SQL语句
		studentDataFrame.registerTempTable("students");

		/**
		 * 针对students临时表,执行SQL语句。 注意：如果没有指明查询字段,查询的结果是乱序的
		 */
		DataFrame sqlDataFrame = sqlContext.sql("select id,name,age from students where age < 19");

		// 显示执行的结果
		sqlDataFrame.show();

		// 将DataFrame转换为RDD
		JavaRDD<Row> sqlRowRDD = sqlDataFrame.javaRDD();

		JavaRDD<Student> sqlStudentRDD = sqlRowRDD.map(new Function<Row, Student>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Student call(Row row) throws Exception {
				int id = row.getInt(0);
				String name = "_" + row.getString(1);
				int age = row.getInt(2);
				return new Student(id, name, age);
			}
		});

		sqlStudentRDD.foreach(new VoidFunction<Student>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Student t) throws Exception {
				System.out.println(t);
			}
		});
	}
}
