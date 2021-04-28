package com.seungwoo.datastreamconnector.sink

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.Calendar

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Flink_RedisSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //读取flink_kafka中的数据把获得到的结果写入redis中提供给后端抽取

    val topic:String = "flink_kafka"

    val per = new Properties
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "toredis")

    // new FlinkKafkaConsumer[String/某个类类型的元数据]()这里会用到官网上的序列化器
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), per)
    //    myConsumer.setStartFromEarliest() // 尽可能从最早的记录开始
    myConsumer.setStartFromLatest()        // 从最新的记录开始
    //    myConsumer.setStartFromTimestamp(0L)  // 从指定的时间开始（毫秒）
    //    myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kafkaSetDatastream: DataStream[String] = env.addSource(myConsumer)

    val dataCountsStream: DataStream[String] = kafkaSetDatastream.map(
      data => {
        ("a", 1L)
      }
    ).keyBy(0).sum(1).map(_._2.toString)
    val cal: Calendar = Calendar.getInstance()

    val host: String = "127.0.0.1"
    val port: Int = 6379

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(host).setPort(port).build()
    dataCountsStream.print()
    dataCountsStream.addSink(new RedisSink[String](conf,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.SET)
      }

      override def getValueFromData(data: String): String = {
        data
      }

      override def getKeyFromData(data: String): String = {
        //实际上你这里利用redis Key不允许重复的特点，会把最新数值
        //重复刷新进入这个key=>"data_counts"后端只需要刷新这个key就可以拿到最新的数值
        //前端显示即可
        "data_counts"
      }
    }))

    env.execute("load data to redis from kafka topic flink_kafka ")
}
}
