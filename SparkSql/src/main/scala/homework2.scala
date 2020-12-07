import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Shi Lei
 * @create 2020-11-26
 */
object homework2 {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("homework2")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //加载csv文件
    val df: DataFrame = spark
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("./csv/homework.csv")

    //注册成表
    df.createOrReplaceTempView("homework2")
    //进行查询
    var result: DataFrame = spark.sql("select * from homework2 ")

    //2.1 定义url连接
    val url="jdbc:mysql://node03:3306/spark?useSSL=false"
    //2.2 定义表名
    val table="user"
    //2.3 定义属性
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")

    //保存结果数据到mysql表中
    result.write.mode("append").jdbc(url,"homework2",properties)

    //关闭
    spark.stop()
  }
}
