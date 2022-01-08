spark-submit \
--class cn.ioceye.core.action.SaveAsTextFileOperation \
--num-executors 3 \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 3 \
--conf spark.ui.port=4040 \
spark-java-1.0.0-jar-with-dependencies.jar