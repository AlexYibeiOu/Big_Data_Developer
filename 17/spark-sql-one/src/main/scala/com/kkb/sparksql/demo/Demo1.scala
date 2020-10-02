package com.kkb.sparksql.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo1 {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("demo1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    val df: DataFrame = spark.read.text(this.getClass.getClassLoader.getResource("person.txt").getPath)
    df.printSchema
    /**
     * root
     * |-- value: string (nullable = true)
     * */
    println("-----")
    println(df.count())
    /** 4 */
    println("-----")
    df.show()
    /**
     * +------------+
     * |       value|
     * +------------+
     * | 1 youyou 38|
     * |   2 Tony 25|
     * |3 laowang 18|
     * |   4 dali 30|
     * +------------+
     * */
  }
}
