package com.seungwoo.datastreamAPI.window.timewindow.tumblingwindow

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object TumbingwindowUse {
  def main(args: Array[String]): Unit = {
    //获取kafka数据使用滚动窗口
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","tumblingwindowuse")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per))

    kafkaStream
      .map(_=>("a",1L))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)
      .map("最近5s的数据量: "+_._2)
      .print()

    env.execute("tubmling process timewindow with keyby")

  }

}
