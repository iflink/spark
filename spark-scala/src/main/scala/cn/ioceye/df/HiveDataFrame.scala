package cn.ioceye.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

// 脚下留心：只支持HiveContext，并且只能提交在集群上运行
object HiveDataFrame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
              setMaster("spark://master:7077").
              setAppName(HiveDataFrame.getClass.getSimpleName())

    val sparkContext = new SparkContext(conf)
    val hiveContext  = new HiveContext(sparkContext)

    hiveContext.sql("drop table if exists student_info")
    hiveContext.sql("create table if not exists student_info(name string, age int) row format delimited fields terminated by ',' stored as textfile")
    hiveContext.sql("load data local inpath 'data/df/student_info.txt' into table student_info")

    hiveContext.sql("drop table if exists student_score")
    hiveContext.sql("create table if not exists student_score(name string, score int) row format delimited fields terminated by ',' stored as textfile")
    hiveContext.sql("load data local inpath 'data/df/student_score.txt' into table student_score")

    val studentDF = hiveContext.sql("select si.name, si.age, ss.score from student_info si left join student_score ss on si.name=ss.name")
    studentDF.show();

    hiveContext.sql("drop table if exists student_info_score")
    studentDF.write.saveAsTable("student_info_score")

    val studentFromHiveDF = hiveContext.table("student_info_score")
    studentFromHiveDF.show()
  }
}