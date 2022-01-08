package cn.ioceye.df;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JSONDataSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			     			.setAppName(GenericSave.class.getSimpleName())
			     			.setMaster("local");
		
		JavaSparkContext context    = new JavaSparkContext(conf);
		SQLContext       sqlContext = new SQLContext(context);
		
		DataFrame studentScoreDF    = sqlContext.read().json("hdfs://master:9000/user/spark/data/student-score.json");
		studentScoreDF.registerTempTable("student_score");
		
		DataFrame goodScoreDF = sqlContext.sql("select name, score from student_score where score >= 80");
		List<String> goodStudentNames = goodScoreDF.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		}).collect();
		
		/**
		 * 学生信息DataFrame
		 * 1.通过hdfs://master:9000/user/spark/data/students.json创建
		 * 2.通过模拟创建
		 */
		List<String> studentInfoList  = new ArrayList<String>(3);
		studentInfoList.add("{\"name\":\"jack\", \"age\":18}");
		studentInfoList.add("{\"name\":\"marry\", \"age\":17}");
		studentInfoList.add("{\"name\":\"leo\", \"age\":19}");
		JavaRDD<String> studentInfoRDD = context.parallelize(studentInfoList, 1);
		
		DataFrame studentInfoDF = sqlContext.read().json(studentInfoRDD);
		studentInfoDF.registerTempTable("student_info");
		
		String sql = "select name, age from student_info where name in('" + goodStudentNames.get(0);
		for(int i=1; i<goodStudentNames.size(); i++) {
			sql += "','" + goodStudentNames.get(i) + "'";
		}
		sql += ")";
		
		DataFrame goodStudentInfoDataFrame = sqlContext.sql(sql);
		goodStudentInfoDataFrame.show();
		
		// 合并2个DataFrame, 实join操作
		JavaPairRDD<String, Tuple2<Integer, Integer>> studentInfoGridRDD 
			= goodScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<String, Integer> call(Row row) throws Exception {
					return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(row.get(1).toString()));
				}
			}).join(goodStudentInfoDataFrame.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<String, Integer> call(Row row) throws Exception {
					return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(row.get(1).toString()));
				}
			}));
		
		JavaRDD<Row> studentsRDD = studentInfoGridRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
			}
		});
		
		List<StructField> structFields = new ArrayList<>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame rsDataFrame = sqlContext.createDataFrame(studentsRDD, structType);
		
		rsDataFrame.printSchema();
		rsDataFrame.show();
	}
}
