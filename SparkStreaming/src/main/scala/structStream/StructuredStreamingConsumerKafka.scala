package com.kaikeba.kafka

import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

//todo: structuredStreaming消费kafka中的topic数据
object StructuredStreamingConsumerKafka {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //todo: 1、创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("structured_streamingApp").master("local[2]")
      .getOrCreate()

    //todo: 2、读取kafka内容
    import  spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","node01:9092,node02:9092,node03:9092")
      .option("subscribe","userinfo")
      .option("startingOffsets","latest")//注意流式处理没有endingOffset
      .option("includeTimestamp",value = true)//输出内容包括时间戳
      .load()

    //todo: 3、数据处理
    val dataSet : Dataset[(String,Timestamp)]=
      df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
        .as[(String,Timestamp)]
    //对kafka的数据进行处理
    val words = dataSet.map(line =>{
      val lineArr=line._1.split(",")
      (lineArr(0),lineArr(1),lineArr(2),line._2)
    }).toDF("primarykey","starttime","value","timestamp")

    //定义窗口
    import  org.apache.spark.sql.functions._
    //根据窗口、开始id分组
    val windowsCount=words
      //.withWatermark("timestamp","1 minutes")
      .groupBy(
        // TODO：设置基于事件时间（event time）窗口 -> timestamp, 每2秒统计最近4秒内数据
        window($"timestamp","4 seconds","2 seconds"),$"primarykey").agg(avg("value"))

    //todo: 4、输出到控制台
    val q = windowsCount
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    q.awaitTermination()
  }
}