package join

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import join.StreamStaticJoinByBroadCast.parseJson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Shi Lei
 * @create 2020-12-07
 */
object Homework1 {

  case class UserInfo1(userID: String, userName: String, userAddress: String)

  case class UserInfo2(userID: String, age: Int)

  //  {"age":20,"userID": "user_2"}
  //  {"age":10,"userID": "user_1"}
//  {"age":30,"userID": "user_3"}
//  {"age":30,"userID": "user_10"}

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("homework1").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val userInfo1: Map[String, UserInfo] = Array(
      UserInfo("user_1", "name_1", "address_1"),
      UserInfo("user_2", "name_2", "address_2"),
      UserInfo("user_3", "name_3", "address_3"),
      UserInfo("user_4", "name_4", "address_4"),
      UserInfo("user_5", "name_5", "address_5")
    ).map(x => x.userID -> x).toMap

    val broadcastValue: Broadcast[Map[String, UserInfo]] = ssc.sparkContext.broadcast(userInfo1)

    //链接kafka

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> "homework")


    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("userVisit"), kafkaParams)
    )

    dStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val value: RDD[UserInfo2] = rdd.map(item => {
          var info: UserInfo2 = null
          try {
            info = JSON.parseObject(item.value(), classOf[UserInfo2])
          } catch {
            case e: Exception => println(item)
          }
          info
        })
        val userInfo2: RDD[(String, UserInfo2)] = value.filter(item => item != null)
          .map(item => item.userID -> item)

        val result: RDD[(String, (UserInfo2, UserInfo))] = userInfo2.map(userInfo2 => {
          val user1: Map[String, UserInfo] = broadcastValue.value
          val key: String = userInfo2._1
          (key, (userInfo2._2, user1.getOrElse(key, null)))
        })
        //打印并且提交
        result.foreach(println)
        offsetCommit(dStream, rdd)


      } //end if
    }

    )
    ssc.start()
    ssc.awaitTermination()

  } //end main


  def offsetCommit(dStream: InputDStream[ConsumerRecord[String, String]], rdd: RDD[ConsumerRecord[String, String]]): Unit = {
    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

}
