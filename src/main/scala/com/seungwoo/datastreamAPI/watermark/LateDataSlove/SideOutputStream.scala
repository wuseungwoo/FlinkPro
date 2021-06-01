package com.seungwoo.datastreamAPI.watermark.LateDataSlove

import java.time.Duration
import org.apache.flink.api.scala._
import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SideOutputStream {
  def main(args: Array[String]): Unit = {
    //读取kafka数据源
    //乱序迟到数据在允许延迟时间的情况下仍存在迟到数据，可以引入侧输出流

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","LatenessData")

    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)
    flinkKafkaConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    /*
    允许迟到数据, 窗口也会真正的关闭, 如果还有迟到的数据怎么办?
    Flink提供了一种叫做侧输出流的来处理关窗之后到达的数据.
     */
    val gson = new Gson()
    val userStream: DataStream[UserBehiver] = kafkaStream
      .map(
        data => {
          gson.fromJson(data, classOf[UserBehiver])
        }
      )

    val userwm: WatermarkStrategy[UserBehiver] = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehiver] {
        override def extractTimestamp(t: UserBehiver, l: Long) = {
          t.time
        }
      })

    val sideStreamTag: OutputTag[(UserBehiver, Long)] = new OutputTag[(UserBehiver, Long)]("window_closed_late_data")
    val resultStream: DataStream[(UserBehiver, Long)] = userStream
      .assignTimestampsAndWatermarks(userwm)
      .map((_, 1L))
      .keyBy(_._1.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .sideOutputLateData(sideStreamTag)
      .reduce(new ReduceFunction[(UserBehiver, Long)] {
        override def reduce(t: (UserBehiver, Long), t1: (UserBehiver, Long)): (UserBehiver, Long) = {
          (t._1, t._2 + t1._2)
        }
      })
    resultStream.getSideOutput(sideStreamTag).print("测输出流输出的聚合结果")
    resultStream.print("主数据流输出")

    env.execute("how to use side out put late data when the window was closed")
  }

}