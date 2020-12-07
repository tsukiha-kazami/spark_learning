package day3

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.RandomRowFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_With_HBase {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("readHbase")
    val sc=new SparkContext(conf)
    //--创建Hbase的环境变量参数
    val hbaseConf=HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","node01,node02,node03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"spark_hbase")

    //设置过滤器，还可以设置起始和结束rowkey
    val scan=new Scan
    scan.setFilter(new RandomRowFilter(0.5f))
    //--设置scan对象，让filter生效
    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    val resultRDD=sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat], classOf[ImmutableBytesWritable],classOf[Result])


    resultRDD.foreach{x=>{
      //--查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      val result=x._2
      //--获取行键
      val rowKey=Bytes.toString(result.getRow)
      val name=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
      val age=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")))
      println(rowKey+":"+name+":"+age)
    }}



    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "spark_hbase_out")
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val outRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.mapPartitions(eachPartitions => {
      eachPartitions.map(eachResult => {
        val result = eachResult._2
        //--获取行键
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
        val put = new Put(Bytes.toBytes(rowKey))
        val immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("name"),
          Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("age"),
          Bytes.toBytes(age))
        (immutableBytesWritable, put)
      })
    })
    outRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }
}