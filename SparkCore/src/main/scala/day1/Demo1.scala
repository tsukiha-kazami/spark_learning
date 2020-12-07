package day1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("word.json").getPath)
    val rdd2: RDD[Option[Any]] = rdd1.map(JSON.parseFull(_))
    rdd2.foreach(println)
  }
}
