package structStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

//todo: StructuredStreaming处理结果保存到mysql当中去
object StructuredStreaming2Mysql {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                      .builder()
                                      .master("local[2]")
                                      .appName("StructuredStreaming2Mysql")
                                      .getOrCreate()
    import spark.implicits._

    //todo: 1、读取数据源
    val lines: DataFrame = spark.readStream
                                .format("socket") // 设置数据源
                                .option("host", "node01")
                                .option("port", 9999)
                                .load

    //todo: 2、数据处理
    val wordCount: DataFrame = lines.as[String]
                                    .flatMap(_.split(" "))
                                    .groupBy("value")
                                    .count()

    //todo: 3、开启查询，结果保存mysql
    val query: StreamingQuery = wordCount.writeStream
      .outputMode("complete")
      //todo: 使用 foreach 的时候, 需要传递ForeachWriter实例, 三个抽象方法需要实现.每个批次的所有分区都会创建 ForeachWriter 实例
      .foreach(new ForeachWriter[Row] {
              var conn: Connection = _
              var ps: PreparedStatement = _

              //初始化 一般用于 打开链接. 返回 false 表示跳过该分区的数据,
              override def open(partitionId: Long, epochId: Long): Boolean = {
                println("open ..." + partitionId + "  " + epochId)
                Class.forName("com.mysql.jdbc.Driver")
                conn = DriverManager.getConnection("jdbc:mysql://node03:3306/spark?useSSL=false", "root", "123456")
                // 插入数据, 当有重复的 key 的时候更新
                val sql = "insert into word_count values(?, ?) on duplicate key update word=?, count=?"
                ps = conn.prepareStatement(sql)

                conn != null && !conn.isClosed && ps != null
              }

            // 把数据写入到连接
            override def process(value: Row): Unit = {
              println("process ...." + value)
              val word: String = value.getString(0)
              val count: Long = value.getLong(1)
              ps.setString(1, word)
              ps.setLong(2, count)
              ps.setString(3, word)
              ps.setLong(4, count)
              ps.execute()
            }

            // 用户关闭连接
            override def close(errorOrNull: Throwable): Unit = {
                  println("close...")
                  ps.close()
                  conn.close()
            }
    }).start

    query.awaitTermination()

  }
}