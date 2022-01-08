package cn.ioceye.df.func

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * 综合案例
 * 统计每天Top3的热点搜索词
 * 
 * nohup hive --service metastore > metastore.log 2>&1 &
 */
object DailyTop3KeyWords {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setMaster("spark://master:7077")
                        .setAppName(DailyTop3KeyWords.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext  = new HiveContext(sparkContext)
    
    val queryParams = Map(
                          "city" -> List("beijing"), 
                          "platform" -> List("android"), 
                          "version" -> List("1.0", "1.2", "1.5", "2.0")
                      )
    // 对查询的数据进行广播,即在每个节点进行备份
    val queryParamsBroadcast = sparkContext.broadcast(queryParams)
    
    val dailyVistorLineRDD   = sparkContext.textFile("hdfs://master:9000/user/spark/data/daily-vistors.txt", 3);
    
    val filterDailyVistorRDD = dailyVistorLineRDD.filter(line => {
      val words = line.split(",")
      
		  var isValidate = false
      if(words.length > 4){
    	  val city     = words(3)
    		val platform = words(4)
			  val version  = words(5)
			  
			  val _queryParams = queryParamsBroadcast.value
			  
			  for(key <- _queryParams.keysIterator
					  if(queryParams(key).contains(city) || queryParams(key).contains(platform) || queryParams(key).contains(version))) {
				  isValidate = true
			  }
      }
      
      isValidate
    })
    
    // (日期_搜索词,用户)
    val dateKeywordUserRDD = filterDailyVistorRDD.map(line => {
      val words = line.split(",")
      
      val date = words(0)
      val keyword  = words(2)
      val userName = words(1)
      
      (date + "_" + keyword, userName) 
    })
    
    val dateKeywordUserGroupByKeyRDD = dateKeywordUserRDD.groupByKey()
    
    // 日期_搜索词 -> 用户, 进行去重操作
    import scala.collection.mutable._
    val dateKeywordUserUVRowRDD = dateKeywordUserGroupByKeyRDD.map(dateKeywordUsers => {
      val keywords = dateKeywordUsers._1.split("_")
      val users    = dateKeywordUsers._2
      
      val userSet = Set[String]()
      for(user <- users) {
        userSet += user
      }
      
      val date    = keywords(0)
      val keyword = keywords(1)
      
      Row(date, keyword, userSet.size)
    })
    
    // 需要应用SparkSQL的开窗函数,必须使用HiveContext
    val strcutFields = Array(
                              StructField("date",    StringType, true),
                              StructField("keyword", StringType, true),
                              StructField("heat",    IntegerType, true)
                       )
    val structType = StructType(strcutFields)
    val dateKeywordUserUVDF = hiveContext.createDataFrame(dateKeywordUserUVRowRDD, structType)
    dateKeywordUserUVDF.write.mode(SaveMode.Overwrite).saveAsTable("daiy_vistor_middle")
    
    val top3Sql = " select date, keyword, heat, rk" + 
                  " from (" +
                  "       select date, keyword, heat, row_number() over (partition by date order by heat desc) as rk from daiy_vistor_middle" +
                  "      ) tmp" +
                  " where rk <= 3"
                  
    val top3DF = hiveContext.sql(top3Sql)
    
    // 对每天热词进行统计
    val top3RDD = top3DF.rdd.map(row => {
      (row(0), row(1) + "_" + row(2))
    })
    
    val top3GroupByDayRDD = top3RDD.groupByKey()
    
    val dailyCountRDD =  top3GroupByDayRDD.map(_keywords => {
      var total: Long = 0
      var keywords = _keywords._1
      
      for(_keyword <- _keywords._2) {
        val heats = _keyword.split("_")
        
        total    += heats(1).toInt
        keywords += "," + _keyword
      }
      (total, keywords)
    })
    
    val dailySortedRDD = dailyCountRDD.sortByKey(false, 3)
    
    val dailyRDD = dailySortedRDD.flatMap(c  => {
      val dailyHeats = c._2.toString.split(",")
      
      var rows = List[Row]()
      for(i <- 1 until dailyHeats.length) {
        val heats = dailyHeats(i).split("_")
        
        val row = Row(dailyHeats(0), heats(0), heats(1).toInt)
        
        rows = row :: rows
      }
      
      rows
    })
    
    val dailyDF = hiveContext.createDataFrame(dailyRDD, structType)
    dailyDF.write.mode(SaveMode.Overwrite).saveAsTable("daiy_vistor_analysis")
  }
}