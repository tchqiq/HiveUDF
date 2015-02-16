提取出udf，方便日后修改
jar包大小为200k
使用时需要将打好的jar包上传到宙斯
然后将宙斯上的hive_udf.jar替换掉即可

此外，请注意：
由于项目为多人维护，并且jar包将用于线上直接生产
每次更新前要先将最新的udf项目pull下来，并且
每次在更新完jar包后请提交到code上来，以免线上的对线上的jar包产生冲突。

**udf 替换jar包的方法：**
上传到宙斯jar包后：
download[hdfs:///hdfs-upload-dir//20140814-141537-hiveudf.jar hiveudf.jar]
替换：
#udf upload
rm -rf /data/1/usr/local/hive/jars/hive_udf.jar
hadoop fs -copyToLocal /hdfs-upload-dir//20140814-141537-hiveudf.jar /data/1/usr/local/hive/jars/hive_udf.jar

