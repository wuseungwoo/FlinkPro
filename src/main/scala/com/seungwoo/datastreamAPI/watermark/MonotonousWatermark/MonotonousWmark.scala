package com.seungwoo.datastreamAPI.watermark.MonotonousWatermark

import org.apache.flink.api.scala._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object MonotonousWmark {
  def main(args: Array[String]): Unit = {
    //读取kafka数据形成数据流，分配水位线时间戳，再做其他处理
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)//方便测试时设置，优先级2（算子>环境>客户端命令>系统配置）

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "countwindow")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per))

    //假定没有数据迟到：理想状态水位线就是最新的数据的时间戳
    //则：此water mark为单调递增水位线
    WatermarkStrategy.forMonotonousTimestamps()

    //kafkaStream...
  }

}
