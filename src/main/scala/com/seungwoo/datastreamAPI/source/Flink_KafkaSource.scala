package com.seungwoo.datastreamAPI.source

import java.util.Properties

import com.seungwoo.utils.MyBytes
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object Flink_KafkaSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val topic: String = "flink_kafka"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    // new FlinkKafkaConsumer[String/某个类类型的元数据]()这里会用到官网上的序列化器
    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
//    myConsumer.setStartFromEarliest() // 尽可能从最早的记录开始
        myConsumer.setStartFromLatest()        // 从最新的记录开始
    //    myConsumer.setStartFromTimestamp(0L)  // 从指定的时间开始（毫秒）
    //    myConsumer.setStartFromGroupOffsets()  // 默认的方法

    val kafkaSetDatastream: DataStream[String] = env.addSource(myConsumer)


    val data_counts: DataStream[String] = kafkaSetDatastream.map(
      data => {
        ("a", 1L)
      }
    ).keyBy(0).sum(1).map(_._2.toString).map("当前数据条数："+_)

    val data_sizes: DataStream[String] = kafkaSetDatastream.map(
      data => {
        ("a", data.getBytes().length.toLong)
      }
    ).keyBy(0).sum(1).map(_._2).map(
      //这里引用了一个工具类根据目前的数据量动态适配数据量的单位
      bytes => {
        MyBytes.getNetFileSizeDescription(bytes)
      }
    ).map("当前收集的数据量:" + _)

    //写入kafka提供后端取数
    val countsPro = new Properties()
    countsPro.setProperty("bootstrap.servers", "localhost:9092")
    data_counts.addSink(new FlinkKafkaProducer[String](
      "data_counts", new SimpleStringSchema(), countsPro
    ))

    val sizesPro = new Properties()
    sizesPro.setProperty("bootstrap.servers", "localhost:9092")
    data_sizes.addSink(new FlinkKafkaProducer[String](
      "data_sizes", new SimpleStringSchema(), sizesPro
    ))


    env.execute("flink for shejiyuan")
  }
}
