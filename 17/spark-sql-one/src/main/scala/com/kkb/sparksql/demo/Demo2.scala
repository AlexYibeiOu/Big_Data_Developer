package com.kkb.sparksql.demo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Demo2 {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("demo2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    //添加隐式转换
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd1=sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath).map(x=>x.split(" "))
    //把rdd与样例类进行关联
    val personRDD=rdd1.map(x=>Person(x(0),x(1),x(2).toInt))

    //把rdd转换成DataFrame
    val df = personRDD.toDF
    df.printSchema()
    /**
     * root
     * |-- id: string (nullable = true)
     * |-- name: string (nullable = true)
     * |-- age: integer (nullable = false)
     * */
    df.show()
    /**
     * +---+-------+---+
     * | id|   name|age|
     * +---+-------+---+
     * |  1| youyou| 38|
     * |  2|   Tony| 25|
     * |  3|laowang| 18|
     * |  4|   dali| 30|
     * +---+-------+---+
     * */
  }
}
//定义一个样例类
case class Person(id:String,name:String,age:Int)