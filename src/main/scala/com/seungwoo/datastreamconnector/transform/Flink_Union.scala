package com.seungwoo.datastreamconnector.transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Flink_Union {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //合并两个相同的流，组成一条新的流
    /*
    connect与 union 区别：
    1.	union之前两个流的类型必须是一样，connect可以不一样
    2.	connect只能操作两个流，union可以操作多个。
     */
    env.setParallelism(1)

    val stream1: DataStream[Int] = env.fromElements(1,2,3,4,5)

    val stream2: DataStream[Int] = env.fromElements(6,7,8,9,10)

    stream1.union(stream2).print()

    env.execute("how to use union")

  }
}
