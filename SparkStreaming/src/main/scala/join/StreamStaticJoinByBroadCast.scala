package join

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//todo: 流与静态数据进行join
object StreamStaticJoinByBroadCast {

  //定义样例类
  case class UserInfo(userID:String,userName:String,userAddress:String)

  def main(args: Array[String]): Unit = {
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("StreamStaticJoinByBroadCast").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //Kafka 参数
    val kafkaParams= Map[String, Object](
        "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "group.id" -> "testGroup")



    //todo: 1) 静态数据: 用户基础信息。 将用户基础信息广播出去。
    val broadcastUserInfo=ssc.sparkContext.broadcast(
      Map(
        "user_1"->UserInfo("user_1","name_1","address_1"),
        "user_2"->UserInfo("user_2","name_2","address_2"),
        "user_3"->UserInfo("user_3","name_3","address_3"),
        "user_4"->UserInfo("user_4","name_4","address_4"),
        "user_5"->UserInfo("user_5","name_5","address_5")
      ))


    //todo: 2)流式数据: 用户发的数据
    /**
      * 获取kafka当中的流式数据，关于用户的json格式的数据
      * */
      val kafkaDStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("userVisit"), kafkaParams)
      ).map(item=>parseJson(item.value())).map(item=>{
        val userID = item.getString("userID")
        val eventTime = item.getString("eventTime")
        val language= item.getString("language")
        val favoriteCount = item.getInteger("favoriteCount")
        val retweetCount = item.getInteger("retweetCount")
        (userID,(userID,eventTime,language,favoriteCount,retweetCount))
      })


    //todo: 3) 流与静态数据做Join (Broadcast Join 方式)
        val result=kafkaDStream.mapPartitions(part=>{
          val userInfo = broadcastUserInfo.value
          part.map(item=>{
            (item._1,(item._2,userInfo.getOrElse(item._1,null)))})
        })

        result.foreachRDD(_.foreach(println))
        ssc.start()
        ssc.awaitTermination()

      }




  /**json解析*/
  def parseJson(log:String):JSONObject={
    var ret:JSONObject=null
    try{
      ret=JSON.parseObject(log)
    }catch {
      //异常json数据处理
      case e:JSONException => println(log)
    }
    ret
  }
}