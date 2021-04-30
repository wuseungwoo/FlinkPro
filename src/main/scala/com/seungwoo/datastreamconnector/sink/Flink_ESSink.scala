package com.seungwoo.datastreamconnector.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Flink_ESSink {
  def main(args: Array[String]): Unit = {
    //Elasticsearch 在项目中主要用于实时数仓,用户画像
    //因此掌握Elasticsearch的数据写入是非常有必要的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val topic:String = "flink_kafka"
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "localhost:9092")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      topic, new SimpleStringSchema(), properties
    ))
    kafkaStream.addSink(new ElasticsearchSink[String]())
  }
}
