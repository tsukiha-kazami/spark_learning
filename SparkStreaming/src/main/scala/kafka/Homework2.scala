package kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Shi Lei
 * @create 2020-12-04
 */
object Homework2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //1、创建StreamingContext对象
    val sparkConf = new SparkConf()
      .setAppName("KafkaDirect10")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val topic = Set("bigdata")
    val kafkaParams = Map(
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "group.id" -> "homework2",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"
    )
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
      )

    kafkaDStream.foreachRDD(rdd => {
      val dataRDD: RDD[String] = rdd.map(_.value())
      dataRDD.foreach(line => {
        println(line)
      })

      //提交offset
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //return
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
