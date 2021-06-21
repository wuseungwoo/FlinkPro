package com.seungwoo.test

import java.text.SimpleDateFormat
import java.util.Calendar

import com.google.gson.Gson
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
   val gson = new Gson()
    val str: String = gson.toJson(new UserTransaction("1001", "wusuengwoo", null, 50000, System.currentTimeMillis()))

//    val str2:String =
//      """
//        |{"client_id":"1001","transaction_amount":60000,"transfer_accounts":null,"time":1623896621038
//      """.stripMargin


    println(gson.fromJson(str, classOf[UserTransaction]))


  }
}
case class UserTransaction(client_id: String, client_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long)
