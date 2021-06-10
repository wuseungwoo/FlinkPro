package com.seungwoo.datastreamAPI.watermark.periodicwatermark.PeriodicWatermarksUse

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object PeriodicWatermarksExa {
  def main(args: Array[String]): Unit = {
    //读取kafka数据，存在延迟数据，周期性获取时间戳构建水位线

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)//方便测试时设置，优先级2（算子>环境>客户端命令>系统配置）

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "countwindow")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per))


    val UserStream: DataStream[UserBehiver] = kafkaStream.map(new Gson().fromJson(_,classOf[UserBehiver]))


    //采用自定义的周期性抽取时间戳的制作watermark
    val wmu: WatermarkStrategy[UserBehiver] = new WatermarkStrategy[UserBehiver] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context) = {
        new MyGenerator
      }
    }.withTimestampAssigner(new SerializableTimestampAssigner[UserBehiver] {
      override def extractTimestamp(t: UserBehiver, l: Long): Long = {
        t.time
      }
    })
    UserStream.assignTimestampsAndWatermarks(wmu)
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
          new Gson().toJson(data._1)+"的条数是："+data._2
        }
      ).print()


  }
}
class MyGenerator extends WatermarkGenerator[UserBehiver]{
  val maxOutOfOrderness = 3500L //乱序程度3.5 秒

  var currentMaxTimestamp: Long = _
  //每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark
  override def onEvent(t: UserBehiver, eventtimestamps: Long, watermarkOutput: WatermarkOutput): Unit = {
    currentMaxTimestamp = Math.max(currentMaxTimestamp,eventtimestamps)
  }


  //周期性的把WaterMark发射出去, 默认周期是200ms
  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
    // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
    watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }
}







case class UserBehiver(id:String,name:String,time:Long)
