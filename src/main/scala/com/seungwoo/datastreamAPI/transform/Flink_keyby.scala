package com.seungwoo.datastreamAPI.transform

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink_keyby {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.fromElements("a","b","c","d","e","f","a","c","e").map(
      new MapFunction[String,(String,Int)] {
        override def map(t: String):(String,Int) = {
          (t,1)
        }
      }
    ).keyBy(0).sum(1).print()
    env.execute("use keyby")
  }

}
