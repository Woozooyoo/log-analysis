package com.atguigu.utils

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster}

import scala.collection.mutable

object ZookeeperUtils{
  def offsetToZookeeper(
      onlineLogDStream: InputDStream[String],
      kafkaCluster: KafkaCluster,
      kafka_group: String): Unit = {

    onlineLogDStream.foreachRDD{
      rdd =>
        // 获取DStream中的offset信息
        // offsetsList: Array[OffsetRange]
        // OffsetRange: topic partition fromoffset untiloffset
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // 遍历每一个offset信息，并更新Zookeeper中的元数据
        // OffsetRange: topic partition fromoffset untiloffset
        for(offsets <- offsetsList){
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          // ack: Either[Err, Map[TopicAndPartition, Short]]
          // Left[Err]
          // Right[Map[TopicAndPartition, Short]]
          val ack = kafkaCluster.setConsumerOffsets(kafka_group, Map((topicAndPartition, offsets.untilOffset)))
          if(ack.isLeft){
            println(s"Error updating the offset to Kafka cluster: ${ack.left.get}")
          }else{
            println(s"update the offset to Kafka cluster: ${offsets.untilOffset} successfully")
          }
        }
    }
  }

  def getOffsetFromZookeeper(
      kafkaCluster: KafkaCluster,
      kafka_group: String,
      kafka_topic_set: Set[String]): Map[TopicAndPartition, Long] = {

    // 创建Map存储Topic和分区对应的offset
    val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]()
    // 获取传入的Topic的所有分区
    // Either[Err, Set[TopicAndPartition]]  : Left(Err)   Right[Set[TopicAndPartition]]
    val topicAndPartitions = kafkaCluster.getPartitions(kafka_topic_set)

    // 如果成功获取到Topic所有分区
    // topicAndPartitions: Set[TopicAndPartition]
    if(topicAndPartitions.isRight){
      // 获取分区数据
      // partitions: Set[TopicAndPartition]
      val partitions = topicAndPartitions.right.get
      // 获取指定分区的offset
      // offsetInfo: Either[Err, Map[TopicAndPartition, Long]]
      // Left[Err]  Right[Map[TopicAndPartition, Long]]
      val offsetInfo = kafkaCluster.getConsumerOffsets(kafka_group, partitions)
      if(offsetInfo.isLeft){
        // 如果没有offset信息则存储0
        // partitions: Set[TopicAndPartition]
        for(top <- partitions)
          topicPartitionOffsetMap += (top->0L)
      }else{
        // 如果有offset信息则存储offset
        // offsets: Map[TopicAndPartition, Long]
        val offsets = offsetInfo.right.get
        for((top, offset) <- offsets)
          topicPartitionOffsetMap += (top -> offset)
      }
    }
    topicPartitionOffsetMap.toMap
  }
}