package com.kkb.sparksql.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo3 {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("demo3")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    val df: DataFrame = spark.read.text(this.getClass.getClassLoader.getResource("person.json").getPath)
    df.printSchema

    /**
     * root
     * |-- value: string (nullable = true)
     **/
    println("-----")
    df.show()
    /**
     * +--------------------+
     * |               value|
     * +--------------------+
     * |  {"name":"Michael"}|
     * |{"name":"Andy", "...|
     * |{"name":"Justin",...|
     * +--------------------+
     **/
  }
}