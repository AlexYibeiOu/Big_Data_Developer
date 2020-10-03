package com.kkb.sparksql.demo

import java.util.regex.{Matcher, Pattern}
import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUDF {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("C:\\Users\\Administrator\\Desktop\\spark-sql-one\\src\\main\\resources\\深圳 链家二手房成交明细.csv")

    frame.createOrReplaceTempView("house_sale")


    session.udf.register("house_udf",new UDF1[String,String] {
      val pattern: Pattern = Pattern.compile("^[0-9]*$")
      override def call(input: String): String = {
        val matcher: Matcher = pattern.matcher(input)
        if(matcher.matches()){
          input
        }else{
          "1990"
        }
      }
    },DataTypes.StringType)

    session.sql("select house_udf(house_age) from house_sale  limit 200").show()
    session.stop()
  }
}

