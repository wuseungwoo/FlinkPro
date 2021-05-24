package com.seungwoo.datastreamAPI.windowfunction


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    //读取kafka数据使用全量聚合函数：ProcessWindowFunction(全窗口函数)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "processwindowfunction")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per))

    /** ProcessWindowFunction:
      *
      * IN   输入类型
      * OUT  输出类型
      * KEY  keyBy中按照Key分组，Key的类型
      * W    窗口的类型
      */

//    kafkaStream
//      .map(_ => (_, 1L))
//      .keyBy(0)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//      .process(new ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {
//        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
//          var sum: Long = 0L
//          for (elem <- elements) {
//            elem._2 + sum = sum
//          }
//          out.collect((key, sum))
//        }
//      }).print()

    env.execute("Process Window Function Use")
  }
}
