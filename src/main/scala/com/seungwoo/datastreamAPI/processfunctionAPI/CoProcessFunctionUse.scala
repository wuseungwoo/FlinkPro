package com.seungwoo.datastreamAPI.processfunctionAPI

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object CoProcessFunctionUse {
  //又称为双流处理：两条数据流的接入同时处理
  //数据流1：kafkaStream
  //数据流2：socketStream
  /*
    为了在两个输入流中实现低层次的操作，
    应用程序可以使用CoProcessFunction，
    这个函数绑定了两个不同的输入流，
    并通过分别调用processElement1(...)和processElement2(...)
    来获取两个不同输入流中的记录。
   */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","co_process_function")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)

    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)

    val userStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )

    val socketStream: DataStream[String] = env.socketTextStream("localhost",8888)

    val conStream: ConnectedStreams[UserTransaction, String] = userStream.connect(socketStream)

    conStream.process(new MyCoPFunction).print()

    env.execute("how to use coProcessFunction")

  }

}
class MyCoPFunction extends CoProcessFunction[UserTransaction,String,String]{
  override def processElement1(value: UserTransaction, ctx: CoProcessFunction[UserTransaction, String, String]#Context, out: Collector[String]): Unit = {
    out.collect(value.transfer_accounts.toString)
  }

  override def processElement2(value: String, ctx: CoProcessFunction[UserTransaction, String, String]#Context, out: Collector[String]): Unit = {
    out.collect(value)
  }
}