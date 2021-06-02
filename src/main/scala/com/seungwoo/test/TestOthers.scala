package com.seungwoo.test

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.{Collector, OutputTag}

//测试侧输出流对主流的数据量的影响：
//结论：主流保持不变，侧输出流只是拿出了符合条件的元素构成了新的流进行输出

object TestOthers {
  def main(args: Array[String]): Unit = {
    val time: Long = System.currentTimeMillis()
  }
}
