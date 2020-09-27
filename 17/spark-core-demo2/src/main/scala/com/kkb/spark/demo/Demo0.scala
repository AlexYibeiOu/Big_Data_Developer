package com.kkb.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Demo0 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(("a", 1), ("a", 2), ("b", 1), ("b", 3), ("c", 1), ("e", 1)), 2)
    println(rdd1.partitions.length) //2
    println(rdd1.partitioner) //None
    val rdd2: RDD[(String, Int)] = rdd1.repartition(4)
    println(rdd2.partitions.length) //4
    println(rdd2.partitioner) //None

    val rdd3: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(4))
    println(rdd3.partitions.length) //4
    println(rdd3.partitioner) //Some(org.apache.spark.HashPartitioner@4)

    println("rdd2:")
    rdd2.glom().mapPartitionsWithIndex((index, it) => {
      it.map(arr=>{
        (index,arr.mkString(","))
      })
    }).foreach(print)
    //(0,(a,2))(1,(b,1),(b,3))(2,(c,1))(3,(a,1),(e,1))

    println()
    println("rdd3:")
    rdd3.glom().mapPartitionsWithIndex((index, it) => {
      it.map(arr=>{
        (index,arr.mkString(","))
      })
    }).foreach(print)
    //(0,)(3,(c,1))(2,(b,1),(b,3))(1,(a,1),(a,2),(e,1))
  }
}