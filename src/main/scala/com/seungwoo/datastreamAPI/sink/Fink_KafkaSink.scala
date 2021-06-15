package com.seungwoo.datastreamAPI.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object Fink_KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


//    // 每隔1000 ms进行启动一个检查点
//    env.enableCheckpointing(1000)
//
//    // 高级选项：
//
//    // 设置模式为exactly-once （这是默认值）
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//
//    // 确保检查点之间有进行500 ms的进度
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//
//    // 检查点必须在一分钟内完成，或者被丢弃
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//
//    // 同一时间只允许进行一个检查点
//
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//
//    //单并行度写入kafka，每秒写入1000条，观察shejiyuan的flink端统计结果
    val myStream: DataStream[String] = env.addSource(new SourceFunction[String] {

      override def run(sourceContext: SourceFunction.SourceContext[String]) = {
        while (true) {
          var message: String =
            """
              |{"client_id":"1001","client_name":"wusuengwoo","transaction_amount":60000,"transfer_accounts":50000,"time":1623401174665}
            """.stripMargin

          sourceContext.collect(message)
          Thread.sleep(1)
        }

      }

      var running: Boolean = true

      override def cancel() = {
        running = false
      }
    })
    val topic:String = "flink_kafka"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val myProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](topic,new SimpleStringSchema(),properties)
    myProducer.setWriteTimestampToKafka(true)

    myStream.addSink(myProducer)

    env.execute("custom stream into tata to flink_kafka")
  }

}
