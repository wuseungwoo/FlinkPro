package com.seungwoo.test

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.{Collector, OutputTag}

//测试侧输出流对主流的数据量的影响

object TestOthers {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //数据源是通过java生成的json进入kafka
    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "side_out_put_data")

    val sideFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per)

    //这里简单设置下，后面会在flink的容错机制中做详细介绍；
    //参考博客：
    /*
    1.  https://www.sohu.com/a/168546400_617676
    2.  https://cpeixin.cn/2020/11/21/Flink%E6%B6%88%E8%B4%B9Kafka%E4%BB%A5%E5%8F%8A%E5%8F%82%E6%95%B0%E8%AE%BE%E7%BD%AE/
     */
    sideFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(sideFKConsumer)

    val ot = new OutputTag[String]("testside")
    val result: DataStream[Long] = kafkaStream.process(new ProcessFunction[String, Long] {
      override def processElement(value: String, ctx: ProcessFunction[String, Long]#Context, out: Collector[Long]): Unit = {
        out.collect(value.toLong)
        if (value.toLong > 5) {
          ctx.output(ot,value)
        }
      }
    })
    result.map("主流： "+_).print()
    result.getSideOutput(new org.apache.flink.streaming.api.scala.OutputTag[String]("testside")).map(">5的流： "+_).print()

    env.execute()
  }
}
