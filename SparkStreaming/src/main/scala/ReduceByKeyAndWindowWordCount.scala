import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * todo: 2秒一个批次，实现每隔4秒统计6秒窗口的数据结果
  */
object ReduceByKeyAndWindowWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("ReduceByKeyAndWindowWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1))


    //todo: 5、每隔4秒统计6秒的数据
    /**
      * 该方法需要三个参数：
      * reduceFunc: (V, V) => V,  ----------> 就是一个函数
      * windowDuration: Duration, ----------> 窗口的大小(时间单位)，该窗口会包含N个批次的数据
      * slideDuration: Duration   ----------> 滑动窗口的时间间隔，表示每隔多久计算一次
      */
    val windowDStream: DStream[(String, Int)] = result.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(6),Seconds(4))

    //todo: 6、打印该批次中所有单词按照次数降序的结果
    windowDStream.print()

    //todo: 7、开启流式计算
    ssc.start()
    ssc.awaitTermination()


  }

}
