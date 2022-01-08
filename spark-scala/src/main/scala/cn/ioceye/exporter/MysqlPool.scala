package cn.ioceye.exporter

import java.io.FileOutputStream
import com.mchange.v2.c3p0.ComboPooledDataSource

/**
 * 持久化到MySQL, 其中PrepareStatement等是不能持久化的。
 * 
 * 原因：查看streaming.output.foreachRDD.txt详情信息
 */
object MysqlPool {
  /**
   * 对象变量
   * 下划线和null的作用是相同的
   * 
   * @transient 注解的变量均不需要序列化
   */
  @transient var cpds: ComboPooledDataSource = _
  
  @transient var os: FileOutputStream = _
  
  try {
    os = new FileOutputStream("system.log", true)
    
    cpds = new ComboPooledDataSource(true)
    cpds.setJdbcUrl("jdbc:mysql://master:3306/test");
    cpds.setDriverClass("com.mysql.jdbc.Driver");
    cpds.setUser("hive");
    cpds.setPassword("2011180062")
    cpds.setMaxPoolSize(200)
    cpds.setMinPoolSize(20)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
  }
  catch {
    case e: Exception => log(e.getMessage)
  }
  
  def saveRDD(table: String, records: Iterator[(String, Int)]): Unit = {
    try{
    	@transient val conn = cpds.getConnection
      @transient val stmt = conn.prepareStatement("insert into " + table + " (keyword, total) values (?, ?)")
      conn.setAutoCommit(false)
      
      records.foreach(_w => {
      	stmt.setString(1, _w._1)
      	stmt.setInt(2, _w._2)
      	stmt.addBatch()
      })
      
      stmt.executeBatch()
      conn.commit()
      
      stmt.close()
      conn.close()
    } 
    catch {
      case e:Exception => log(e.getMessage)
    }
  }
  
  def log(message: String): Unit = {
    try{
      os.write((message + "\n").getBytes)
      os.flush()
    }
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
}