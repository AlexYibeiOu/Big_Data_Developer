package com.kkb.sparksql.demo

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CSVOperate {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")//时间转换
      .option("header", "true")//表示第一行数据都是head(字段属性的意思)
//    .option("multiLine", true)//表示数据可能换行
      .load("/Users/yibeiou/IdeaProjects/spark-sql-one/src/main/resources/data")

    frame.createOrReplaceTempView("job_detail")
//    session.sql("select job_name,job_url,job_location,job_salary,job_company,job_experience,job_class,job_given,job_detail,company_type,company_person,search_key,city from job_detail ").show(10)

    session.sql("select count(*) from job_detail").show(10)

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "phpmysql")

    frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/kkb_big_data?serverTimezone=GMT&useSSL=false&useUnicode=true&characterEncoding=UTF-8", "kkb_big_data.jobdetail_copy", prop)

  }
}