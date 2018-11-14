package com.atguigu.online

import java.util.Date

import com.atguigu.model.StartupReportLogs
import com.atguigu.utils._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Online {

  def main(args: Array[String]): Unit = {
    val streamingContext = StreamingContext.getActiveOrCreate(
      PropertiesUtils.loadProperties("streaming.checkpoint.path"), createSSCFunc())

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createSSCFunc(): () => _root_.org.apache.spark.streaming.StreamingContext = {
    () => {

      // 1. 配置sparkConf
      val sparkConf = new SparkConf().setAppName("online").setMaster("local[*]")
      // 配置spark任务优雅的停止
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 配置每秒钟从kafka主题的分区消费的消息条数
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 指定序列化方式为Kryo序列化方式
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 指定Kryo序列化方式的Registrator
      sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator")

      // 2. 创建streamingContext并开启checkpoint机制
      val interval = PropertiesUtils.loadProperties("streaming.interval").toLong
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval))

      //开启checkpoint机制
      val checkPointPath = PropertiesUtils.loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      // 3. 获取kafka配置信息
      val kafkaBrokers = PropertiesUtils.loadProperties("kafka.broker.list")
      val kafkaTopic = PropertiesUtils.loadProperties("kafka.topic")
      val kafkaTopicSet: Set[String] = Set(kafkaTopic)
      val kafkaGroup = PropertiesUtils.loadProperties("kafka.groupId")

      val kafkaParams = Map(
        "bootstrap.servers" -> kafkaBrokers,
        "group.id" -> kafkaGroup
      )

      // 4. 创建kafkaCluster，获取Zookeeper里之前可能有的offsete信息，
      // 目的是让我们新建的DirectDStream能够从上次消费到的位置进行消费
      val kafkaCluster = new KafkaCluster(kafkaParams)

      // topicAndPartitionOffset: Map[TopicAndPartition, Long]
      val topicAndPartitionOffset = ZookeeperUtils.getOffsetFromZookeeper(
                                                    kafkaCluster,
                                                    kafkaGroup,
                                                    kafkaTopicSet)

      // 5. 创建DirectDStream
      // kafkaDStream: DStream[RDD[String]]
      val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream
        [String, String, StringDecoder, StringDecoder, String](
          streamingContext,
          kafkaParams,
          topicAndPartitionOffset,
          //回调函数 每消费到一条message 都会调用这个回调函数 对message进行处理
          (mess: MessageAndMetadata[String, String]) => mess.message
        )

      //保存一份, 这一次运行的数据
      kafkaDStream.checkpoint(Duration(10000))

      // 6. 逻辑代码
      // kafkaDStream: DStream[RDD[String]]  String -> StartupLog
      // kafkaFilterDStream: DStream[RDD[String]]  String -> StartupLog  所有的数据都是符合过滤条件的
      val kafkaFilterDStream = kafkaDStream.filter {
        log =>
          var success = true
          // 滤除垃圾数据（三种日志以外的数据）
          if (!log.contains("appVersion") && !log.contains("currentPage") && !log.contains("errorMajor")) {
            success = false
          }

          // 针对启动上报日志，如果无法转换为实例或者关键字段为空，就进行滤除
          if (log.contains("appVersion")) {
            val startupLog: StartupReportLogs = JsonUtils.json2StartupLog(log)
            if (startupLog == null || startupLog.getCity == null || startupLog.getUserId == null ||
              startupLog.getAppId == null) {
              success = false
            }
          }

          success
      }

      kafkaFilterDStream.foreachRDD {
        rdd =>
          rdd.foreachPartition {
            // iterable[startupLog1, startupLog2, ...]
            items =>
              val table = HBaseUtils.getHBaseTabel(PropertiesUtils.getProperties())
              // item : startupLog
              for (item <- items) {
                val startupLog = JsonUtils.json2StartupLog(item)
                val date = new Date(startupLog.getStartTimeInMs)
                // dateString : yyyy-MM-dd
                val dateString = DateUtils.dateToString(date)
                val rowKey = dateString + "_" + startupLog.getCity
                table.incrementColumnValue(
                  Bytes.toBytes(rowKey),
                  Bytes.toBytes("StatisticData"),
                  Bytes.toBytes("clickCount"),
                  1L
                )

                println(rowKey)
              }
          }
      }

      // 7. 更新Zookeeper的offset信息
      ZookeeperUtils.offsetToZookeeper(kafkaDStream, kafkaCluster, kafkaGroup)

      streamingContext
    }
  }

}
