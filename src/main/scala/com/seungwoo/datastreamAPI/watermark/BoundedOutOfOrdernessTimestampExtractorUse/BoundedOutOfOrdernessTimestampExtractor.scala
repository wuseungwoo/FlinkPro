package com.seungwoo.datastreamAPI.watermark.BoundedOutOfOrdernessTimestampExtractorUse

import java.time.Duration
import org.apache.flink.api.scala._
import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object BoundedOutOfOrdernessTimestampExtractor {
  def main(args: Array[String]): Unit = {
    //读取kafka数据，使用•	BoundedOutOfOrdernessTimestampExtractor
    //对延迟数据的流生成允许吃到一定时间的watermark
    //使用开发人员产生的watermark对窗口的关闭进行操作

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)//方便测试时设置，优先级2（算子>环境>客户端命令>系统配置）

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "countwindow")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per))

    val gson = new Gson()
    val userStream: DataStream[UserBehiver] = kafkaStream.map(
      data => {
        gson.fromJson(data, classOf[UserBehiver])
      }
    )


    val booo: WatermarkStrategy[UserBehiver] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehiver] {
        override def extractTimestamp(t: UserBehiver, l: Long) = {
          t.time
        }
      })

    userStream
      .assignTimestampsAndWatermarks(booo)
        .map(
          data=>{
            (data,1L)
          }
        )
      .keyBy(_._1.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(8)))
      .reduce(new ReduceFunction[(UserBehiver, Long)] {
        override def reduce(t: (UserBehiver, Long), t1: (UserBehiver, Long)): (UserBehiver, Long) = {
          (t._1,t._2+t1._2)
        }
      } )
      .map(
        data=>{
          gson.toJson(data._1)+"的条数是："+data._2
        }
      ).print()

    env.execute("kafka user data(json string) count with BoundedOutOfOrdernessTimestampExtractor")
  }

  case class UserBehiver(id:String,name:String,time:Long)
}
