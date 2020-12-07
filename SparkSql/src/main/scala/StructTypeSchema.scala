import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo；通过动态指定dataFrame对应的schema信息将rdd转换成dataFrame
object StructTypeSchema {

  def main(args: Array[String]): Unit = {
    //todo: 1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
                                          .appName("StructTypeSchema")
                                          .master("local[2]")
                                          .getOrCreate()

    //todo: 2、获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    //todo：3、读取文件数据
    val data: RDD[Array[String]] = sc.textFile("E:\\person.txt").map(x=>x.split(" "))

    //todo: 4、将rdd与Row对象进行关联
    val rowRDD: RDD[Row] = data.map(x=>Row(x(0),x(1),x(2).toInt))

    //todo: 5、指定dataFrame的schema信息
    //这里指定的字段个数和类型必须要跟Row对象保持一致
    val schema=StructType(
      StructField("id",StringType)::
        StructField("name",StringType)::
        StructField("age",IntegerType)::Nil
    )

    //todo: 6、通过RDD[Row]和StructType来构建DataFrame
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)

    //打印schema
    dataFrame.printSchema()
    //显示结果
    dataFrame.show()

    //注册成表
    dataFrame.createTempView("user")

    //sql查询分析
    spark.sql("select * from user").show()


    //todo: 关闭
    spark.stop()

  }

}
