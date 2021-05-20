package com.seungwoo.datastreamAPI.transform

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Flink_filter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements(1, 2, 3, 4, 5, 6).filter(
      //FilterFunction[Int]这里的参数泛型是输入参数的泛型,默认是返回Boolean类型
      new FilterFunction[Int] {
        override def filter(t: Int):Boolean = {
          t % 2 == 0
        }
      }
    ).print()
    env.execute("how to use filter")
  }
}
