# HiveWarehouse



## UDF,UDTF,UDAF
* UDF:常见的函数类型，可以操作单个数据行，且产生一个数据行作为输出，大多数函数为这一类。
* UDAF:用户自定义聚集函数，接收多个输入数据行，产生一个输出数据行。
* UDTF:用户自定义表生成函数，用于操作单个输入数据行，产生多个数据行（一张表）作为输出。

### 部署方式
* 将jar包上传到HDFS指定路径下: /user_ext/ad_engine/udf_warehouse
* 在hive命令行创建函数: CREATE FUNCTION ad_angine_md5 AS 'com.adengine.udf.Md5Udf' using jar 'viewfs://c9//user_ext/ad_engine/udf_warehouse/ad_engine_udf_warehouse-1.0-jar-with-dependencies.jar';
  全局使用需要加库名,default.ad_angine_md5
* 如需更改，只需要更改相应的jar包，重新上传即可