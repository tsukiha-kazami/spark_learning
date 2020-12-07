package day1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * mapPartitionsWithIndex算子，一个分区内处理，几个分区就执行几次，返回带有分区号的结果集
     **/
    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, it) => {
      println("执行") //执行两次
      it.map((index, _))
    })
    val result: Array[(Int, Int)] = value.collect()
    result.foreach(x => print(x + "\t"))
  }
}
