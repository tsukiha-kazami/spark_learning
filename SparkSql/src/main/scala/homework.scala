import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author Shi Lei
 * @create 2020-11-23
 */
object homework {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("StructTypeSchema")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    val rowRDD: RDD[Row] = sc
      .textFile(this.getClass.getClassLoader.getResource("person.txt").getPath)
      .map(x => x.split(" "))
      .map(x => Row(x(0), x(1), x(2).toInt))

    val schema = StructType(
        StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil
    )

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)

    //注册成表
    dataFrame.createTempView("person")
    //sql查询分析
    spark.sql("select * from person limit 3").show()
    spark.sql("select count(1) from person ").show()

    // 关闭
    spark.stop()

  }
}
