package com.seungwoo.project.highlevelpro.LogDataAndNetflowStatistics

import java.time.Duration

import com.seungwoo.bean.UserBehavior
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PVCountWithWindow {
  //实时统计每小时内的网站PV=每隔一个小时统计当前小时的实时pv
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

    //逻辑代码的编写没有思路的时候可以使用逻辑sql化的方式去写
    userBehaviorStream
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .keyBy(_.behavior)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .process(new MyProcessFunction)
      .print("use ProcessWindowFunction=>")


    env.execute()
  }

  class MyProcessFunction extends ProcessWindowFunction[UserBehavior, Long, String, TimeWindow] {
    //定义合适的状态：键控状态：map value list reduce aggregate
    //使用ValueState装载上一批的pv统计值,这里不需要获取上一批的pv统计值，所以不需要记忆在存储里
    //    var pvCount: ValueState[Long] = _
    //
    //    override def open(parameters: Configuration): Unit = {
    //      //初始化ValueState
    //      val pvcountstateDesc = new ValueStateDescriptor[Long]("pvcountstate", classOf[Long])
    //
    //      pvCount = getRuntimeContext.getState(pvcountstateDesc)
    //
    //    }
    var counts: Long = 0L

    override def process(key: String, context: Context, elements: Iterable[UserBehavior], out: Collector[Long]): Unit = {
      for (elem <- elements.iterator.toList) {
        //其实这里可以获取到key，输出可以是(key,counts)
        //实现对不同的可以进行分组求和
        counts = counts + 1L
      }
      out.collect(counts)
    }
  }

}
