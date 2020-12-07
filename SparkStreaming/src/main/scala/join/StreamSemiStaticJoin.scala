package join

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis


//todo: 数据流与半静态数据进行join
object StreamSemiStaticJoin {

  //定义样例类
  case class UserInfo(userID:String,userName:String,userAddress:String)

  def main(args: Array[String]): Unit = {
    //设置日志等级
    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("StreamSemiStaticJoin").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //Kafka 参数
    val kafkaParams= Map[String, Object](
        "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "group.id" -> "testGroup")




    //todo: 1)流式数据: 用户发的数据
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


    //todo: 2) 流与静态数据做Join (Broadcast Join 方式)
        val result=kafkaDStream.mapPartitions(part=>{

          val redisCli = connToRedis("node01", 6379, 3000)
          part.map(item => {
            (item._1, (item._2, redisCli.hmget(item._1, "userID", "name", "address")))
          })
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

  /**连接到redis*/
  def connToRedis(redisHost:String,redisPort:Int,timeout:Int): Jedis ={
    val redisCli=new Jedis(redisHost,redisPort,timeout)
    redisCli.auth("123456")
    redisCli.connect()
    redisCli
  }
}
