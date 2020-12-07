import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

//todo: 自定义UDAF函数：多对一
object CustomSparkUDAF {
  def main(args: Array[String]): Unit = {

    //todo: 1、构建SparkSession
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomSparkUDAF")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    //todo: 2、加载csv文件
      val df: DataFrame = session
                                  .read
                                  .format("csv")
                                  .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                                  .option("header", "true")
                                  .option("multiLine", true)
                                  .load("./secondHandHouse/深圳链家二手房成交明细.csv")

    //todo: 3、注册成表
    df.createOrReplaceTempView("house_sale")
    session.sql("select floor from house_sale limit 30").show()

    //todo: 4、注册UDAF函数
    session.udf.register("udaf",new MyAverage)

    //todo: 5、然后按照楼层进行分组，求取每个楼层的平均成交金额
    session.sql("select floor, udaf(house_sale_money) from house_sale group by floor").show()


   //todo: 6、关闭
    session.stop()
  }
}

//todo: 自定义UDAF函数

class MyAverage extends UserDefinedAggregateFunction {
  /**
    * 可以看出，继承这个类之后，要重写里面的八个方法, 每个方法代表的含义是：
          inputSchema：输入数据的类型
          bufferSchema：产生中间结果的数据类型
          dataType：最终返回的结果类型
          deterministic：确保一致性（输入什么类型的数据就返回什么类型的数据），一般用true
          initialize：指定初始值
          update：每有一条数据参与运算就更新一下中间结果（update相当于在每一个分区中的运算）
          merge：全局聚合(将每个分区的结果进行聚合)
          evaluate：计算最终的结果
    */

  // todo: 输入数据的类型
  def inputSchema: StructType = StructType(StructField("floor", DoubleType) :: Nil)

  // todo: 产生中间结果的数据类型
  def bufferSchema: StructType = {
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  }

  //todo: 最终返回的结果类型
  def dataType: DataType = DoubleType

  //todo: 对于相同的输入是否一直返回相同的输出。
  def deterministic: Boolean = true

  //todo: 指定初始值
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 用于存储不同类型的楼房的总成交额
    buffer(0) = 0D
    // 用于存储不同类型的楼房的总个数
    buffer(1) = 0L
  }

  //todo:  相同Execute间的数据合并。（分区内聚合）
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一条数据参与运算就更新一下中间结果
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //todo: 不同Execute间的数据合并（全局聚合)
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //todo:  计算最终结果
  def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getLong(1)
}