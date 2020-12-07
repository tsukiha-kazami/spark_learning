package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    //写法一
    val seqOp = (partSum: Int, value: Int) => {
      partSum + value
    }
    val combOp = (partSumThis: Int, partSumOther: Int) => {
      partSumThis + partSumOther
    }
    rdd2.aggregateByKey(0)(seqOp, combOp).foreach(x => print(x + "\t"))
    println

    //写法二
    //柯里化的好处，可以进行类型推断
    rdd2.aggregateByKey(0)((partSum, value) => {
      partSum + value
    }, (partSumThis, partSumOther) => {
      partSumThis + partSumOther
    }).foreach(x => print(x + "\t"))
    println

    //写法三：简化
    val result: RDD[(String, Int)] = rdd2.aggregateByKey(0)(_ + _, _ + _)
    result.foreach(x => print(x + "\t")) //(spark,1)	(hadooop,1)	(hello,2)
  }
}