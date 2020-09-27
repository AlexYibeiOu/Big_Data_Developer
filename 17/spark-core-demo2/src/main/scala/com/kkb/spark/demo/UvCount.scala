package com.kkb.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UvCount {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile("/Users/yibeiou/Downloads/IT/Big_Data_Developer/17/第十七章 spark计算框架/第2节 sparkCore第二次课/SparkCore第二次课/工程代码/spark-core-demo2/src/main/resources/access.log")

    //4、获取所有的ip地址
    val ipsRDD: RDD[String] = dataRDD.map(x=>x.split(" ")(0))

    //5、对ip地址进行去重
    val distinctRDD: RDD[String] = ipsRDD.distinct()

    //6、统计uv
    val uv: Long = distinctRDD.count()
    println(s"uv:$uv")
    sc.stop()
  }
}