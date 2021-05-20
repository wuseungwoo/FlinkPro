package com.seungwoo.datastreamAPI.window.timewindow.slidingwindow

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SlidingwindowUse {
  def main(args: Array[String]): Unit = {
    //读取kafka数据使用slidingwindow
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","slidingwindow")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per))

    kafkaStream
      .map(_=>("a",1L))
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(1)))
      .sum(1)
      .map("每隔一秒统计最近5秒内的数据条数: "+_._2)
      .print()

    env.execute("use sliding process time window")
  }

}
