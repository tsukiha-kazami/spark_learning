import java.util.regex.{Matcher, Pattern}
import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo: 自定义UDF函数：一对一
object CustomSparkUDF {
  def main(args: Array[String]): Unit = {

    //todo: 1、构建SparkSession
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomSparkUDF")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //todo: 2、加载csv文件
    val df: DataFrame = spark
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("./深圳链家二手房成交明细.csv")




    //todo: 3、注册成表
    df.createOrReplaceTempView("house_sale")

    //todo: 4、自定义UDF函数
    spark.udf.register("house_udf",new UDF1[String,String] {
      val pattern: Pattern = Pattern.compile("^[0-9]*$")
      override def call(input: String): String = {
        val matcher: Matcher = pattern.matcher(input)
        if(matcher.matches()){
          input
        }else{
          "1990"
        }
      }
    },DataTypes.StringType)

    //todo: 5、使用自定义UDF函数进行查询
    spark.sql("select house_udf(house_age) from house_sale  limit 200").show(100)

    spark.stop()
  }
}