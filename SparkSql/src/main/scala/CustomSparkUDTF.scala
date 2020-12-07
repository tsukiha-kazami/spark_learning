import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


//todo: 自定义UDTF函数：多对一
object CustomSparkUDTF {
  def main(args: Array[String]): Unit = {

    //todo: 1、构建SparkSession
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomSparkUDTF")
    val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    //todo: 2、加载csv文件
      val df: DataFrame = session
                                  .read
                                  .format("csv")
                                  .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                                  .option("header", "true")
                                  .option("multiLine", true)
                                  .load("./csv/深圳链家二手房成交明细.csv")

    //todo: 3、注册成表
    df.createOrReplaceTempView("house_sale")


    //todo: 4、注册udtf函数
     // 注册utdf算子，这里无法使用sparkSession.udf.register()，注意包全路径
     session.sql("create temporary function mySplit as 'com.kaikeba.sql.myUDTF'")

    //todo: 5、将part_place（部分地区）以空格切分进行展示
    session.sql("select part_place, mySplit(part_place,' ') from house_sale limit 50").show()

   //todo: 6、关闭
    session.stop()
  }
}

//todo: 自定义UDTF函数

class myUDTF extends GenericUDTF {

  // 该函数的作用:①输入参数校验，只能传递一个参数 ②指定输出的字段名和字段类型
  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    //参数个数校验，判断参数是否为2
    if (args.length != 2) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only two argument")
    }
    // 用于验证参数的类型，判断第一个参数是不是字符串参数
    if (args(0).getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }

    //初始化表结构
    //创建数组列表存储表字段
    val fieldNames: ArrayList[String] = new ArrayList[String]()
    var fieldOIs: ArrayList[ObjectInspector] = new ArrayList[ObjectInspector]()
    // 输出字段的名称
    fieldNames.add("newColumn")

    // 这里定义的是输出列字段类型
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    //将表结构两部分聚合在一起
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  // 用于处理数据，入参数组objects里只有1行数据,即每次调用process方法只处理一行数据
  override def process(objects: Array[AnyRef]): Unit = {
    //获取数据
    val data: String = objects(0).toString
    //获取分隔符
    val splitKey: String = objects(1).toString()
    //切分数据
    val words: Array[String] = data.split(splitKey)

    //遍历写出
    words.foreach(x => {
      //将数据放入集合
      var tmp: Array[String] = new Array[String](1)
      tmp(0) = x
      //写出数据到缓冲区
      forward(tmp)
    })
  }

  override def close(): Unit = {

  }
}