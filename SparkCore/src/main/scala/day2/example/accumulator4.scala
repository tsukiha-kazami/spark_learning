package day2.example

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object accumulator4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
    val acc = new MyAccumulatorV2
    //注册累加器
    sc.register(acc)

    rdd1.foreach(x => {
      acc.add(x)
    })
    println("main:  " + acc.value)

    /**main:  Map(sum -> 15.0, count -> 5.0, avg -> 3.0) */
  }
}

class MyAccumulatorV2 extends AccumulatorV2[Int, Map[String, Double]] {
  var map: Map[String, Double] = Map[String, Double]()

  //判断累加的值是不是空
  override def isZero: Boolean = map.isEmpty

  //如何把累加器copy到Executor
  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    val accumulator = new MyAccumulatorV2
    accumulator.map ++= map
    accumulator
  }

  //重置值
  override def reset(): Unit = {
    map = Map[String, Double]()
  }

  //分区内的累加
  override def add(v: Int): Unit = {
    map += "sum" -> (map.getOrElse("sum", 0d) + v)
    map += "count" -> (map.getOrElse("count", 0d) + 1)
  }

  //分区间的累加，累加器最终的值
  override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
    other match {
      case o: MyAccumulatorV2 =>
        this.map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
        this.map += "count" -> (map.getOrElse("count", 0d) + o.map.getOrElse("count", 0d))
      case _ =>
    }
  }

  override def value: Map[String, Double] = {
    map += "avg" -> map.getOrElse("sum", 0d) / map.getOrElse("count", 1d)
    map
  }
}