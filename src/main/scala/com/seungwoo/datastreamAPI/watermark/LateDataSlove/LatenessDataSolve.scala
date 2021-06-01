package com.seungwoo.datastreamAPI.watermark.LateDataSlove

import java.time.Duration

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object LatenessDataSolve {
  def main(args: Array[String]): Unit = {
    /*
    已经添加了wartemark之后, 仍有数据会迟到怎么办?
    Flink的窗口, 允许迟到数据.
	  当触发了窗口计算后, 会先计算当前的结果, 但是此时并不会关闭窗口.
	  以后每来一条迟到数据, 则触发一次这条数据所在窗口计算(增量计算).
	  那么什么时候会真正的关闭窗口呢?  wartermark超过了窗口结束时间+等待时间
     */
    //允许迟到只能运用在event time上
    /*
    在1.12之前默认的时间语义是处理时间
    从1.12开始, Flink内部已经把默认的语义改成了事件时间
     */
    //读取kafka数据，包装流后使用窗口+自带的简单实现周期性抽取时间戳的类
    //允许迟到数据等待时间：5s
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","LatenessData")

    val flinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)
    flinkKafkaConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(flinkKafkaConsumer)

    val gson = new Gson()
    val userBshiverStream: DataStream[UserBehiver] = kafkaStream
      .map(
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

    userBshiverStream
      //使用周期获取时间戳函数忍受延迟时间为：5s
      //使用事件时间的时间戳
      //按照事件数据的id分组
      //使用滚动事件时间窗口，且窗口大小为8s
      //允许数据迟到的时间为5s，迟到5s内的时间会触发窗口更新统计数据
      //当watermark > 窗口结束时间 + 允许数据迟到时间   ，过来的事件数据将不被计算直接丢弃
      //在window后面使用 .allowedLateness(Time.seconds(5)) 等5s的数据因为要计算进入原有窗口更新数据统计结果
      //在 .allowedLateness(Time.seconds(5)) 的后面使用窗口函数对窗口数据进行统计（增量聚合函数）
      .assignTimestampsAndWatermarks(booo)
      .map(
        data=>{
          (data,1L)
        }
      )
      .keyBy(_._1.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(8)))
      .allowedLateness(Time.seconds(5))
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

    env.execute("For Lateness data we could use allowedLateness at the back of window")
  }
}

case class UserBehiver(id:String,name:String,time:Long)