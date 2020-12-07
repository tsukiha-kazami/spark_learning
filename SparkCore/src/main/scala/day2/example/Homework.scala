package day2.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Shi Lei
 * @create 2020-11-19
 */
object Homework {

  def main(args: Array[String]): Unit = {
    //统计出访问URL最少的6个
    val sparkConf: SparkConf = new SparkConf().setAppName("Homework").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val rdd: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("access.log").getPath)
    rdd.filter(x => x.split(" ").length > 10) //过滤小于10的行
      .map(_.split(" ")(10)) //取第十个元素
      .filter(_.startsWith("\"http")) //过滤掉不是http的请求
      .groupBy(x => x) //根据本身分组，
      .map(x => x._1 -> x._2.size) //将key作为key，数组长度 作为value
      .sortBy(_._2, true) //按照value就是访问数量，升序
      .take(6) //取前6
      .foreach(println) //打印
    sc.stop()
  }
}
