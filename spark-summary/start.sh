spark-submit \
--class cn.uway.summary.MDTWeekSummary \
--name mdtweek-summary \
--master yarn \
--deploy-mode cluster \
--executor-memory 12G \
--driver-memory 2G \
--num-executors 3 \
--executor-cores 3 \
uwayspark-1.0.0.binary.jar 20190526