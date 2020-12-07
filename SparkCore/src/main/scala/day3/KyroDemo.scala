package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KyroDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("keyo")
      .setMaster("local[*]")
      //替换默认序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //注册需要使用的kryo序列化自定义类
      .registerKryoClasses(Array(classOf[MySearcher]))

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hadoop yarn", "hadoop hdfs", "c"))
    val rdd2: RDD[String] = MySearcher("hadoop").getMathcRddByQuery(rdd1)
    rdd2.foreach(println)
  }
}

case class MySearcher(val query: String) {
  def getMathcRddByQuery(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }
}