import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//定义一个样例类
case class User(id: String, name: String, age: Int)

//todo: RDD、DataFrame、DataSet互相转换案例
object SparkConversion {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkConversion")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //todo: 隐式转换
    import spark.implicits._

    //todo: 加载数据
    val rdd = sc.textFile("D:\\person.txt").map(x => x.split(" "))

    //todo: 把rdd与样例类进行关联
    val userRDD = rdd.map(x => User(x(0), x(1), x(2).toInt))

    //1. rdd转df
    val df1: DataFrame = userRDD.toDF
    df1.show

    //2. RDD转DS
    val ds1: Dataset[User] = userRDD.toDS
    ds1.show

    //3. DF转RDD
    val rdd1: RDD[Row] = df1.rdd
    println(rdd1.collect.toList)

    //4. DS转RDD
    val rdd2: RDD[User] = ds1.rdd
    println(rdd2.collect.toList)

    //5. DS转DF
    val df2: DataFrame = ds1.toDF
    df2.show()

    //6. DF转DS
    val ds2: Dataset[User] = df2.as[User]
    ds2.show()

    spark.stop()
  }
} 