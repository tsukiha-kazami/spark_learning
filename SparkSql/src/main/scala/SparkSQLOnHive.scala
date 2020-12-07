import org.apache.spark.sql.{DataFrame, SparkSession}

//todo: 通过代码开发实现sparksql操作hive表
object SparkSQLOnHive {
  def main(args: Array[String]): Unit = {
    //todo: 1、构建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
                                          .appName("HiveSupport")
                                          .master("local[2]")
                                          .enableHiveSupport() //开启对hive的支持
                                          .getOrCreate()

    //todo: 2、直接使用sparkSession去操作hivesql语句

      //2.1 创建一张hive表
      spark.sql("create table if not exists default.kaikeba(id string,name string,age int) row format delimited fields terminated by ','")

      //2.2 加载数据到hive表中
      spark.sql("load data local inpath './data/kaikeba.txt' into table default.kaikeba ")

      //2.3 查询
      spark.sql("select * from default.kaikeba").show()
      spark.sql("select count(*) from default.kaikeba").show()

    //todo: 关闭SparkSession
    spark.stop()
  }
}
