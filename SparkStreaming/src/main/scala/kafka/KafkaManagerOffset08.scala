//package kafka
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//
///**
//  * 使用直连方式 SparkStreaming连接kafka0.8获取数据
//  * 手动将偏移量数据保存到zookeeper中
//  */
//object KafkaManagerOffset08 {
//
//  def main(args: Array[String]): Unit = {
//
//    //todo:1、创建SparkConf 提交到集群中运行 不要设置master参数
//    val conf = new SparkConf().setAppName("KafkaManagerOffset08").setMaster("local[4]")
//
//    //todo: 2、设置SparkStreaming，并设定间隔时间
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    //todo:3、指定相关参数
//
//        //指定组名
//        val groupID = "consumer-kaikeba"
//        //指定消费者的topic名字
//        val topic = "wordcount"
//        //指定kafka的broker地址
//        val brokerList = "node01:9092,node02:9092,node03:9092"
//
//        //指定zookeeper的地址，用来存放数据偏移量数据，也可以使用Redis MySQL等
//        val zkQuorum = "node01:2181,node02:2181,node03:2181"
//
//        //创建Stream时使用的topic名字集合，SparkStreaming可同时消费多个topic
//        val topics: Set[String] = Set(topic)
//
//        //创建一个 ZKGroupTopicDirs 对象，就是用来指定在zk中的存储目录，用来保存数据偏移量
//        val topicDirs = new ZKGroupTopicDirs(groupID, topic)
//
//        //获取 zookeeper 中的路径 "/consumers/consumer-kaikeba/offsets/wordcount"
//        val zkTopicPath = topicDirs.consumerOffsetDir
//
//        //构造一个zookeeper的客户端 用来读写偏移量数据
//        val zkClient = new ZkClient(zkQuorum)
//
//        //准备kafka的参数
//        val kafkaParams = Map(
//          "metadata.broker.list" -> brokerList,
//          "group.id" -> groupID,
//          "enable.auto.commit" -> "false"
//        )
//
//
//    //todo:4、定义kafkaStream流
//    var kafkaStream: InputDStream[(String, String)] = null
//
//
//    //todo:5、获取指定的zk节点的子节点个数
//    val childrenNum = getZkChildrenNum(zkClient,zkTopicPath)
//
//
//    //todo:6、判断是否保存过数据 根据子节点的数量是否为0
//    if (childrenNum > 0) {
//
//      //构造一个map集合用来存放数据偏移量信息
//      var fromOffsets: Map[TopicAndPartition, Long] = Map()
//
//      //遍历子节点
//      for (i <- 0 until childrenNum) {
//
//        //获取子节点  /consumers/consumer-kaikeba/offsets/wordcount/0
//        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/$i")
//        // /wordcount-----0
//        val tp = TopicAndPartition(topic, i)
//
//        //获取数据偏移量  将不同分区内的数据偏移量保存到map集合中
//        //  wordcount/0 -> 1001
//        fromOffsets += (tp -> partitionOffset.toLong)
//      }
//
//      // 泛型中 key：kafka中的key   value：hello tom hello jerry
//      //创建函数 解析数据 转换为（topic_name, message）的元组
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
//
//      //todo:7、利用底层的API创建DStream 采用直连的方式(之前已经消费了，从指定的位置消费)
//       kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
//
//    } else {
//      //todo:7、利用底层的API创建DStream 采用直连的方式(之前没有消费，这是第一次读取数据)
//      //zk中没有子节点数据 就是第一次读取数据 直接创建直连对象
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    }
//
//    //todo:8、直接操作kafkaStream
//    //依次迭代DStream中的kafkaRDD 只有kafkaRDD才可以强转为HasOffsetRanges  从中获取数据偏移量信息
//    //之后是操作的RDD 不能够直接操作DStream 因为调用Transformation方法之后就不是kafkaRDD了获取不了偏移量信息
//
//    kafkaStream.foreachRDD(kafkaRDD => {
//      //强转为HasOffsetRanges 获取offset偏移量数据
//      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      //获取数据
//      val lines: RDD[String] =kafkaRDD.map(_._2)
//
//      //todo：9、接下来就是对RDD进行操作 触发action
//      lines.foreachPartition(patition => {
//        patition.foreach(x => println(x))
//      })
//
//      //todo: 10、手动提交偏移量到zk集群上
//      for (o <- offsetRanges) {
//        //拼接zk路径   /consumers/consumer-kaikeba/offsets/wordcount/0
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//
//        //将 partition 的偏移量数据 offset 保存到zookeeper中
//        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
//      }
//    })
//
//    //开启SparkStreaming 并等待退出
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//  /**
//    * 获取zk节点上的子节点的个数
//    * @param zkClient
//    * @param zkTopicPath
//    * @return
//    */
//  def getZkChildrenNum(zkClient:ZkClient,zkTopicPath:String):Int ={
//
//    //查询该路径下是否有子节点，即是否有分区读取数据记录的读取的偏移量
//    // /consumers/consumer-kaikeba/offsets/wordcount/0
//    // /consumers/consumer-kaikeba/offsets/wordcount/1
//    // /consumers/consumer-kaikeba/offsets/wordcount/2
//
//    //子节点的个数
//    val childrenNum: Int = zkClient.countChildren(zkTopicPath)
//
//    childrenNum
//  }
//}
