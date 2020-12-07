package day1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * mapPartitions算子，一个分区内处理，几个分区就执行几次，优于map函数，常用于时间转换，数据库连接
      **/
    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(it => {
      println("执行") //分区2次，共打印2次
      it.map(x => x * 2)
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t"))
  }
}
