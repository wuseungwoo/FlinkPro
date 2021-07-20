package com.seungwoo.project.lowlevelpro

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


object PVCounts {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val csvStream: DataStream[String] = env.readTextFile("D:\\Idea\\Idea_workspace\\FlinkPro\\src\\main\\scala\\com\\seungwoo\\project\\page_view\\UserBehavior.csv")

    csvStream.filter(
      datas => {
        datas.contains("pv")
      }
    ).map(data => {
      ("pv", 1L)
    }).keyBy(0).sum(1).map("网站访问量为" + _._2).print()
    env.execute()
  }
}
