import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object homework {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("homework1").setMaster("local[2]")
    val context = new SparkContext(conf)
    context.setLogLevel("warn")
    val scc: StreamingContext = new StreamingContext(context, Seconds(2))
    val dStream : ReceiverInputDStream[String] = scc.socketTextStream("node01", 9999)
    dStream.flatMap(_.split(" +")).map(_->1).reduceByKey(_+_).print()

    scc.start()
    scc.awaitTermination()

      
  }
}
