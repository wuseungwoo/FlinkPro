package com.seungwoo.datastreamAPI.watermark.periodicwatermark.BoundedOutOfOrdernessTimestampExtractorUse

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
    /*
    •	BoundedOutOfOrdernessTimestampExtractor抽象类实现AssignerWithPeriodicWatermarks接口的extractTimestamp及getCurrentWatermark方法，同时声明抽象方法extractAscendingTimestamp供子类实现
    •	BoundedOutOfOrdernessTimestampExtractor的构造器接收maxOutOfOrderness参数用于指定element允许滞后(t-t_w，t为element的eventTime，t_w为前一次watermark的时间)的最大时间，在计算窗口数据时，如果超过该值则会被忽略
    •	BoundedOutOfOrdernessTimestampExtractor的extractTimestamp方法会调用子类的extractTimestamp方法抽取时间，如果该时间大于currentMaxTimestamp，则更新currentMaxTimestamp；getCurrentWatermark先计算potentialWM，如果potentialWM大于等于lastEmittedWatermark则更新lastEmittedWatermark(currentMaxTimestamp - lastEmittedWatermark >= maxOutOfOrderness，这里表示lastEmittedWatermark太小了所以差值超过了maxOutOfOrderness，因而调大lastEmittedWatermark)，最后返回Watermark(lastEmittedWatermark)
     */
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
}
case class UserBehiver(id:String,name:String,time:Long)

