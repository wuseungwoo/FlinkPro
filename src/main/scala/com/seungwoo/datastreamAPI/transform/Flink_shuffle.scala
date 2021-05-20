package com.seungwoo.datastreamAPI.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Flink_shuffle {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    //当并行度不为一的时候，将对同一组数据随机进行分配使得每次的输出顺序和分区不同

    env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).shuffle.print()

    env.execute("what is shuffle")
  }

}
