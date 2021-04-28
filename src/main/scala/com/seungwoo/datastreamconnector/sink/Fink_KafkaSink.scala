package com.seungwoo.datastreamconnector.sink

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
select
 a.average_daily_asset_range as average_daily_asset_range,
 a.group_id as group_id,
 a.median as median,
 a.client_count as client_count,
 a.total_purchase_amount as total_purchase_amount,
 if(a.total_purchase_amount=0,0.00,cast((a.total_purchase_amount/f.total_entrust_amount)*1.00 as decimal(20,6))) as subscription_ratio,
 b.buied_counts,
 if(b.buied_counts=0,'0.00%',cast(cast((cast(b.buied_counts as double)/a.client_count)*100.00 as decimal(10,2)) as varchar)||'%') as buied_ratio
from total_entrust_amount as f,first_of_tb as a
join buy_counts as b on a.group_id = b.group_id
order by a.group_id

            """.stripMargin

          sourceContext.collect(message)
          Thread.sleep(1000)
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
