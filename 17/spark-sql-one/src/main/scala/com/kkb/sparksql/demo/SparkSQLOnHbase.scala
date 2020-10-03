//package com.kkb.sparksql.demo
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
//import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
//import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
//
//object SparkSQLOnHbase {
//  def main(args: Array[String]): Unit = {
//    //创建sparksession
//    val spark = SparkSession.builder()
//      .appName("SparkSQLOnHbase")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .master("local[*]")
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("WARN")
//    import spark.implicits._
//    val hconf: Configuration = HBaseConfiguration.create
//    hconf.set(HConstants.ZOOKEEPER_QUORUM, "cdh01.cm,cdh02.cm,cdh03.cm")
//    hconf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
//    //一定要创建这个hbaseContext, 因为后面写入时会用到它，不然空指针
//    val hBaseContext = new HBaseContext(spark.sparkContext, hconf)
//
//    //定义映射的catalog
//    val catalog: String = "{" +
//      "       \"table\":{\"namespace\":\"default\", \"name\":\"spark_hbase\"}," + //空间名和表名
//      "       \"rowkey\":\"key\"," + //row固定写法
//      "       \"columns\":{" + //f0等为读出数据列名
//      "         \"f0\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"}," + //rowKey列写法
//      "         \"f1\":{\"cf\":\"info\", \"col\":\"addr\", \"type\":\"string\"}," + //其他列写法
//      "         \"f2\":{\"cf\":\"info\", \"col\":\"age\", \"type\":\"boolean\"}," +
//      "         \"f3\":{\"cf\":\"info\", \"col\":\"name\", \"type\":\"string\"}" +  //注意最后一行没有逗号
//      //      "         \"f2\":{\"cf\":\"cf2\", \"col\":\"f2\", \"type\":\"double\"}," +    //其他数据类型
//      //      "         \"f3\":{\"cf\":\"cf3\", \"col\":\"f3\", \"type\":\"float\"}," +
//      //      "         \"f4\":{\"cf\":\"cf4\", \"col\":\"f4\", \"type\":\"int\"}," +
//      //      "         \"f5\":{\"cf\":\"cf5\", \"col\":\"f4\", \"type\":\"bigint\"}," +
//      //      "         \"f6\":{\"cf\":\"cf6\", \"col\":\"f6\", \"type\":\"smallint\"}," +
//      //      "         \"f7\":{\"cf\":\"cf7\", \"col\":\"f7\", \"type\":\"string\"}," +
//      //      "         \"f8\":{\"cf\":\"cf8\", \"col\":\"f8\", \"type\":\"tinyint\"}" +
//      "       }" +
//      "     }"
//
//
//    //读取Hbase数据
//    val df: DataFrame = spark.read
//      .format("org.apache.hadoop.hbase.spark")
//      .option(HBaseTableCatalog.tableCatalog, catalog)
//      .load()
//    df.show(10)
//
//
//    val catalogCopy: String = catalog.replace("spark_hbase", "spark_hbase_copy")
//    //数据写入Hbase
//    df.write
//      .format("org.apache.hadoop.hbase.spark")
//      .option(HBaseTableCatalog.tableCatalog, catalogCopy)
//      .mode(SaveMode.Overwrite)
//      .save()
//  }
//}
