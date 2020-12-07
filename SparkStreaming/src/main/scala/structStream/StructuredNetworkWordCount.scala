package structStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

//todo: 通过 structuredStreaming接受socket数据实现单词计数
object StructuredNetworkWordCount {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    //todo: 1、构建SparkSession对象
    val spark = SparkSession.builder
                            .appName("StructuredNetworkWordCount")
                            .master("local[2]")
                            .getOrCreate()

    //todo: 2、导入隐式转换
    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    //todo: 3、读取socket数据
    val lines = spark.readStream.format("socket")
                                .option("host", "node01")
                                .option("port", 9999)
                                .load()

    // todo: 4、Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    //todo: 5、Generate running word count
    val wordCounts = words.groupBy("value").count()

    //todo:6、启动查询, 把结果打印到控制台
    val query:StreamingQuery = wordCounts.writeStream
                                          .outputMode("complete")
                                          .format("console")
                                          .start()

    query.awaitTermination()


    }
  }