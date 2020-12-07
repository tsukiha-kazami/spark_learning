package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cogroup").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 10), ("b", 10), ("a", 20), ("d", 10)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 30), ("b", 20), ("c", 10)))

    /**
     * (c,(CompactBuffer(),CompactBuffer(10)))
     * (b,(CompactBuffer(10),CompactBuffer(20)))
     * (a,(CompactBuffer(10, 20),CompactBuffer(30)))
     * (d,(CompactBuffer(10),CompactBuffer()))
     */
    rdd1.cogroup(rdd2).foreach(println)
  }
}