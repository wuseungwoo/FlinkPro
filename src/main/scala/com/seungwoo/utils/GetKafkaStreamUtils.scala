package com.seungwoo.utils

import org.apache.flink.api.scala._
import com.seungwoo.bean.ConstantClazz
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object GetKafkaStreamUtils {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def getKafkaStream():DataStream[String]={
    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","SimpleCondititonsConsumer")


    val myFlinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](ConstantClazz.topic,new SimpleStringSchema(),per)
    myFlinkKafkaConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFlinkKafkaConsumer)
    return kafkaStream
  }
}
