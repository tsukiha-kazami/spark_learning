package day1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object SparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val tuples: Array[(String, Int)] = sc.textFile(ClassLoader.getSystemResource("word.csv").getPath)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    tuples.foreach(println)
    sc.stop()
  }
}
