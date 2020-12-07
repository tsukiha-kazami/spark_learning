package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("join").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 10), ("b", 10), ("a", 20), ("d", 10)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 30), ("b", 20), ("c", 10)))
    //内连接 (a,(10,30))	(b,(10,20))	(a,(20,30))
    rdd1.join(rdd2).foreach(x => print(x + "\t"))
    println()

    //左链接(b,(10,Some(20)))	(d,(10,None))	(a,(10,Some(30)))	(a,(20,Some(30)))
    rdd1.leftOuterJoin(rdd2).foreach(x => print(x + "\t"))
    println()
    //右链接(c,(None,10))	(a,(Some(10),30))	(b,(Some(10),20))	(a,(Some(20),30))
    rdd1.rightOuterJoin(rdd2).foreach(x => print(x + "\t"))
    println()
    //全链接(b,(Some(10),Some(20)))	(c,(None,Some(10)))	(d,(Some(10),None))	(a,(Some(10),Some(30)))	(a,(Some(20),Some(30)))
    rdd1.fullOuterJoin(rdd2).foreach(x => print(x + "\t"))
  }
}