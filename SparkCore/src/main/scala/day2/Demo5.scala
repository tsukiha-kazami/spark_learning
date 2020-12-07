package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    //方式一
    rdd2.foldByKey(0)((x, y) => {x + y}).foreach(x => print(x + "\t"))
    println()

    //简写
    val result: RDD[(String, Int)] = rdd2.foldByKey(0)(_ + _)
    result.foreach(x => print(x + "\t")) //(spark,1)	(hadooop,1)	(hello,2)
  }
}