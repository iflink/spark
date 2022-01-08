spark-submit \
--class cn.ioceye.sql.DataFrameCreate \
--num-executors 3
--driver-memory 100m
--executor-memory 100m
--executor-cores 3
--files /usr/local/bigdata/hive-2.1.1/conf/hive-site.xml \
--driver-class-path /usr/local/bigdata/hive-2.1.1/lib/mysql-connector-java.jar
spark-scala.jar