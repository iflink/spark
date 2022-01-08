# 01. spark-summary

目标：主要解决mdt天数据汇总到周数据的项目，针对指标的聚合以及一些运算，汇总结果供impala查询。

# 02. 技术概述
项目主要用于scala + maven + spark开发。

- 开发工具：idea + maven。
- spark技术：spark-core、spark-broadcast、spark-dataset。

# 03. 开发详情
- 数据源：/user/impala/lte_hd/clt_mro_gc_day_l/year=?/month=?/day=?
	> 接入1周的数据，即周一到周日。
- mapPartitions -> (grid + earfcn, mdt)
- reduceByKey -> (grid + earfcn, mdt)
- persist
- mapPartitions -> (grid, mdt)
- groupByKey -> Iterator[(grid, Iterable[MDTDay])]
- mapPartitions -> Row
- createDataFrame -> dataframe
- write.mode(SaveMode.Overwrite).format("parquet")
      .partitionBy("year", "month", "day").save("path")
![process](https://img-blog.csdnimg.cn/20190616131054596.png)