package com.kkb.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("ahello", "bhadooop", "chello", "dspark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    rdd2.sortByKey(true).foreach(x => print(x + "\t")) //(dspark,1)	(chello,1)	(bhadooop,1)	(ahello,1)
  }
}