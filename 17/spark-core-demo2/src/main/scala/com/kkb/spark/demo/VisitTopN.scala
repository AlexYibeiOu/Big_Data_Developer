package com.kkb.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object VisitTopN {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile("/Users/yibeiou/Downloads/IT/Big_Data_Developer/17/第十七章 spark计算框架/第2节 sparkCore第二次课/SparkCore第二次课/工程代码/spark-core-demo2/src/main/resources/access.log")
    //4、先对数据进行过滤
    val filterRDD: RDD[String] = dataRDD.filter(x =>x.split(" ").length >10)
    //5、获取每一个条数据中的url地址链接
    val urlsRDD: RDD[String] = filterRDD.map(x=>x.split(" ")(10))
    //6、过滤掉不是http的请求
    val fUrlRDD: RDD[String] = urlsRDD.filter(_.contains("http"))
    //7、把每一个url计为1
    val urlAndOneRDD: RDD[(String, Int)] = fUrlRDD.map(x=>(x,1))
    //8、相同的url出现1进行累加
    val result: RDD[(String, Int)] = urlAndOneRDD.reduceByKey(_+_)
    //9、对url出现的次数进行排序----降序
    val sortRDD: RDD[(String, Int)] = result.sortBy(_._2,false)
    //10、取出url出现次数最多的前5位
    val top5: Array[(String, Int)] = sortRDD.take(5)
    top5.foreach(println)
    sc.stop()
  }
}