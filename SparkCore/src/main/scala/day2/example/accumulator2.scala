package day2.example

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object accumulator2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
    val acc: LongAccumulator = sc.longAccumulator("acc")
    rdd1.foreach(x => {
      acc.add(1)
      println("rdd:  "+acc.value)
    })
    println("-----")
    println("main:  "+acc.count)

    /**
      * rdd:  1
      * rdd:  1
      * rdd:  2
      * rdd:  2
      * rdd:  3
      * -----
      * main:  5
      * */
  }
}