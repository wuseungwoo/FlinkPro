package com.seungwoo.project.highlevelpro.LogDataAndNetflowStatistics

import java.time.Duration

import com.seungwoo.bean.UserBehavior
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UVCountWithWindow {
  //指定时间范围内网站独立访客数（UV）的统计
  //每隔一小时统计去重的客户数
  //543462,1715,1464116,pv,1511658000
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //数据样例：543462,1715,1464116,pv,1511658000

    val csvStream: DataStream[String] = env.readTextFile("D:\\Idea\\Idea_workspace\\FlinkPro\\src\\main\\scala\\com\\seungwoo\\project\\lowlevelpro\\UserBehavior.csv")

    val userBehaviorStream: DataStream[UserBehavior] = csvStream
      .map(
        data => {
          val dataArray: Array[String] = data.split(",")
          UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toLong, dataArray(3).toString, dataArray(4).toLong)
        }
      )
    //增加水印策略：定期周期获取
    val boo: WatermarkStrategy[UserBehavior] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
      new SerializableTimestampAssigner[UserBehavior] {
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
          element.timestamp
        }
      }
    )

    //sql思想处理，按照id分组后求和
    //分组--键控状态的MapState
    //求和reduce，sum，process等
    //什么是键控的Map状态？每一个对应的key都在map(key,value)中维护着其对应的key和value
    userBehaviorStream
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .keyBy(_.behavior)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .process(new MyProcessWindowFunnction)
      .print("uv_count===>")

    env.execute()
  }

  class MyProcessWindowFunnction extends ProcessWindowFunction[UserBehavior,Long,String,TimeWindow]{
    var uvState:MapState[Long,Long] = _
    override def open(parameters: Configuration): Unit = {
      val uvmapStateDesc = new MapStateDescriptor[Long,Long]("uvsmaptate",classOf[Long],classOf[Long])
      uvState = getRuntimeContext.getMapState(uvmapStateDesc)
    }
    override def process(key: String, context: Context, elements: Iterable[UserBehavior], out: Collector[Long]): Unit = {

      for (elem <- elements) {
        uvState.put(elem.userID,1L)
      }

      out.collect(uvState.keys().spliterator().estimateSize())

      uvState.clear()
    }
  }
}
