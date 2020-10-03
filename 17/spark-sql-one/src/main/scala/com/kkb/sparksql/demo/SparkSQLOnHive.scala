package com.kkb.sparksql.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLOnHive {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("SparkSQLOnHive")
      .master("local[*]")
      .enableHiveSupport() //启用hive
      .getOrCreate()

    val df: DataFrame = spark.sql("select * from student")
    df.show()
    //直接写表方式
    df.write.saveAsTable("student1")
    //通过insert into插入
    spark.sql("insert into student1 select * from student")
  }
}