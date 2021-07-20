package com.seungwoo.project.lowlevelpro

import java.util
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object UvCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val csvStream: DataStream[String] = env.readTextFile("D:\\Idea\\Idea_workspace\\FlinkPro\\src\\main\\scala\\com\\seungwoo\\project\\page_view\\UserBehavior.csv")

    val hset: util.HashSet[Long] = new util.HashSet[Long]()
    val UvCounts: DataStream[Int] = csvStream.filter(
      data => {
        data.contains("pv")
      }
    ).map(
      data => {
        val userID: Long = data.split(",")(0).toLong
        hset.add(userID)
        hset.size()
      }
    )
    UvCounts.map("当前的独立访客数："+_).print()

    env.execute("独立访客数统计")
  }

}

//543462,1715,1464116,pv,1511658000
case class UserBehavior(userId: Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
