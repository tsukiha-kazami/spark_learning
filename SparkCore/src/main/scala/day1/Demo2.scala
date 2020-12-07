package day1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("object").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    rdd1.saveAsObjectFile("hdfs://cdh01.cm/test")

    val rdd2: RDD[Nothing] = sc.objectFile("hdfs://cdh01.cm/test")
    rdd2.foreach(println)
  }
}
