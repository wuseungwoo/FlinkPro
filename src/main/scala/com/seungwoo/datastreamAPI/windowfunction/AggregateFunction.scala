package com.seungwoo.datastreamAPI.windowfunction

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object AggregateFunction {
  def main(args: Array[String]): Unit = {
    //kafka数据源+time window+Aggregate Function
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","aggregatefunction")
    val fkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)

    val kafkaStream: DataStream[String] = env.addSource(fkConsumer)

    kafkaStream.flatMap(_.split("\\w+")).map(
      data=>{
        (data,1L)
      }
    ).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(new AggregateFunction[Tuple2[String,Long],Tuple2[String,Long],Tuple2[String,Long]] {
        override def createAccumulator() = {
          //创建累加器  初始化
          (" ", 0L)
        }

        override def add(in: (String, Long), acc: (String, Long)) = {
          (in._1, in._2 + acc._2)
        }

        override def getResult(acc: (String, Long)) = {
          acc
        }

        override def merge(acc: (String, Long), acc1: (String, Long)) = {
          (acc._1, acc._2 + acc1._2)
        }
      }).print()
    env.execute("use aggregate function")
  }

}
