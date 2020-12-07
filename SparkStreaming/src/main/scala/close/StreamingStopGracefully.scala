package close

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//todo: 优雅的关闭sparkStreaming程序
object StreamingStopGracefully {

  def main(args: Array[String]): Unit = {
    //todo: 构建StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", () => createSSC())

    //todo: 启动检测线程
    new Thread(new MonitorStop(ssc)).start()

    //todo: 启动流处理程序
    ssc.start()
    ssc.awaitTermination()
  }


  //todo: 创建StreamingContext对象，编写业务处理逻辑
  def createSSC(): StreamingContext = {

    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {

      //当前批次内容的计算
      val sum: Int = values.sum

      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)

      //累加结果返回
      Some(sum + lastStatu)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingStopGracefully")

    //todo: 设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("warn")

    ssc.checkpoint("./ck")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))

    //累加
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)

    //打印啥结果
    wordAndCount.print()

    //返回streamingContext
    ssc
  }


}
