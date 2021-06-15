package com.seungwoo.datastreamAPI.processfunctionAPI

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, JoinedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ProcessJoinFunctionUse {
  //这里我们可以和transform包下的connect算子和union算子做一个对比：
  /*
  connect与 union 区别：
    1.union之前两个流的类型必须是一样，connect可以不一样
    2.connect只能操作两个流，union可以操作多个

  connect => coProcessFunction
  union => processJoinFunction
   */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val per1 = new java.util.Properties()
    per1.setProperty("bootstrap.servers","localhost:9092")
    per1.setProperty("group.id","stream1")
    val topic1:String = "flink_kafka1"

    val per2 = new java.util.Properties()
    per2.setProperty("bootstrap.servers","localhost:9092")
    per2.setProperty("group.id","stream2")
    val topic2:String = "flink_kafka2"

    val flinkKafkaCon1: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic1,new SimpleStringSchema(),per1)
    val flinkKafkaCon2: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic2,new SimpleStringSchema(),per2)

    flinkKafkaCon1.setStartFromLatest()
    flinkKafkaCon2.setStartFromLatest()

    val kafkaStream1: DataStream[String] = env.addSource(flinkKafkaCon1)
    val kafkaStream2: DataStream[String] = env.addSource(flinkKafkaCon2)

    val userStream1: DataStream[UserTransaction] = kafkaStream1.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )
    val userStream2: DataStream[UserTransaction] = kafkaStream2.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )
    val joinedStream: JoinedStreams[UserTransaction, UserTransaction] = userStream1.join(userStream2)

    joinedStream.where(_.client_id).equalTo(_.client_id)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[UserTransaction,UserTransaction,UserTransaction] {
        override def join(in1: UserTransaction, in2: UserTransaction): UserTransaction = {
          UserTransaction(in1.client_id+"|"+in2.client_id,in1.client_name+"|"+in2.client_name,in1.transaction_amount+in2.transaction_amount,in1.transfer_accounts+in2.transfer_accounts,System.currentTimeMillis())
        }
      })
      .print()

    env.execute("how to use process join function")
  }
}
