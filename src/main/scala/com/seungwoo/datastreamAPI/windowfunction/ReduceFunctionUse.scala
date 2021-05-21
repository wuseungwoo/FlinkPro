package com.seungwoo.datastreamAPI.windowfunction

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ReduceFunctionUse {
  def main(args: Array[String]): Unit = {
    //参见com.seungwoo.datastreamAPI.window.countwindow.CountWindowUse
    //读取kafka数据使用countwindow
    //体会根据传入参数的不同,countwindow的类型也不同
    //这里再count window之后使用了reduce function做聚合
    /*
      增量聚合函数:基于中间状态计算结果的，窗口中只维护中间状态结果值，
      不需要缓存原始的数据。

      x以前的聚合结果，y下一个数据，输入输出都为同类型数据
      .reduce(new ReduceFunction[Sensor] {
      override def reduce(x: Sensor, y: Sensor): Sensor = {
        Sensor(x.id, x.timestamp + 1, y.temperature + 1)
      }
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)//方便测试时设置，优先级2（算子>环境>客户端命令>系统配置）

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "countwindow")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per))

    //尝试解析数据 a-1 b-2 a-3 b-3...

    var key: String = " "
    var value: Long = 0L
    kafkaStream
      .map(
        data => {
          if (data.contains("-")) {
            key = data.split("-")(0)
            value = data.split("-")(1).toLong
          }
          (key, value)
        }
      )
      .keyBy(0)
      .countWindow(3)
      //.countWindow(5,1)
      .reduce(new ReduceFunction[(String, Long)] {
      override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = {
        (t._1, t._2 + t1._2)
      }
    })
      .map("每3条数据按照key分组统计值：" + _)
      .print()

    env.execute()
  }
}
