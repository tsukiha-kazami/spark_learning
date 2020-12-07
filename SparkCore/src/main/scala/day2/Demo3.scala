package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    val result: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    result.foreach(x => print(x + "\t"))
    //(spark,CompactBuffer(1))	(hadooop,CompactBuffer(1))	(hello,CompactBuffer(1, 1))
    result.map(x => (x._1, x._2.size)).foreach(x => print(x + "\t")) //(spark,1)	(hadooop,1)	(hello,2)
  }
}