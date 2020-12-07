package structStream


import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 * @author Shi Lei
 * @create 2020-12-07
 */
object Day3homework2 {

  case class Role(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .appName("Day3homework2").master("local[2]")
      .getOrCreate()

    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
      .option("subscribe", "userVisit")
      .option("startingOffsets", "latest") //注意流式处理没有endingOffset
      //      .option("includeTimestamp", value = true) //输出内容包括时间戳
      .load()

    val value: Dataset[Role] = dataFrame.selectExpr("CAST(value AS STRING)").as[String]
      .map(line => line.split(",")).map(x => Role(x(0), x(1).toInt))

    value.createOrReplaceTempView("homework2")

    val frame: DataFrame = sparkSession.sql("select name,count(*),avg(age)  from homework2 group by name")

    val q = frame
      .writeStream
//      .outputMode("update")
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()
    q.awaitTermination()
  } //end main


}
