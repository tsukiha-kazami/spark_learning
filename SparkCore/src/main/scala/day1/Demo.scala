package day1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Shi Lei
 * @create 2020-11-18
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * 传入分区数为1，结果数序打印
     * 传入分区数大于1，结果顺序不定，因为数据被打散在N个分区里
     * */
    val rdd1: RDD[Int] = sc.parallelize(1.to(10), 1)
    print("rdd1->:")
    rdd1.foreach(x => print(x + "\t"))
    println("")

    val rdd2=sc.parallelize(List(1,2,3,4,5),2)
    print("rdd2->:")
    rdd2.foreach(x => print(x + "\t"))
    println("")

    val rdd3=sc.parallelize(Array("hadoop","hive","spark"),1)
    print("rdd3->:")
    rdd3.foreach(x => print(x + "\t"))
    println("")

    val rdd4=sc.makeRDD(List(1,2,3,4),2)
    print("rdd4->:")
    rdd4.foreach(x => print(x + "\t"))
    println("")

    sc.stop()
  }
}
