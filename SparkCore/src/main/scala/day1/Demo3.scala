package day1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * map算子，一共有多少元素就会执行多少次，和分区数无关，修改分区数进行测试
     **/
    val rdd: RDD[Int] = sc.parallelize(1.to(5), 1)
    val mapRdd: RDD[Int] = rdd.map(x => {
      println("执行") //一共被执行5次
      x * 2
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t"))
  }
}
