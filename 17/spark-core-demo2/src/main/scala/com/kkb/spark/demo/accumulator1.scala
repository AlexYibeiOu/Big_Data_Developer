package com.kkb.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object accumulator1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
    var a = 1
    rdd1.foreach(x => {
      a += 1
      println("rdd:  "+a)
    })
    println("-----")
    println("main:  "+a)

    /**
      * rdd:  2
      * rdd:  2
      * rdd:  3
      * rdd:  3
      * rdd:  4
      * -----
      * main:  1
      * */
  }
}