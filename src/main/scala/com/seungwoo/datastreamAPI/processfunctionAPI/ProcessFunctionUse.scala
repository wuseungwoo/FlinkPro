package com.seungwoo.datastreamAPI.processfunctionAPI

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ProcessFunctionUse {
  def main(args: Array[String]): Unit = {
    //读取kafka，包装流使用process function
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

    //转换为样例类的流
    val userStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        val gson = new Gson()
        gson.fromJson(data, classOf[UserTransaction])
      }
    )
    userStream.process(new ProcessFunction[UserTransaction,UserTransaction] {
      override def processElement(value: UserTransaction, ctx: ProcessFunction[UserTransaction, UserTransaction]#Context, out: Collector[UserTransaction]): Unit = {
        //做个过滤：交易额>5万以上的，或者转账20w以上的
        if(value.transaction_amount>50000L || value.transfer_accounts > 200000L){
          out.collect(value)
        }
      }
    }).print()

    env.execute()
  }

}

case class UserTransaction(client_id: String, client_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long)
