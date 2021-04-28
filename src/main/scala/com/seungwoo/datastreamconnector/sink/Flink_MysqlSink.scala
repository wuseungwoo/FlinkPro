package com.seungwoo.datastreamconnector.sink

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.seungwoo.utils.Flink_MysqlUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


//这里可以考虑使用Flink CDC！
object Flink_MysqlSink {
  def main(args: Array[String]): Unit = {
    //写入mysql如何写入？
    //首先想到的是使用自定义sink的方式写入
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val topic: String = "flink_kafka"
    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      topic,new SimpleStringSchema(), properties
    ))

    kafkaStream.addSink(new MysqlSink)

    env.execute()
  }

  class MysqlSink extends RichSinkFunction[String] {
    var connection: Connection = _
    var statement: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      val sql: String =
        """
          |insert into a(id) values(?);
        """.stripMargin
      connection = Flink_MysqlUtils.getMysqlConnection()
      statement = connection.prepareStatement(sql)
    }

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      statement.setString(1,value)
      statement.execute()
    }

    override def close(): Unit = {
      Flink_MysqlUtils.close(statement)
      Flink_MysqlUtils.close(connection)
    }
  }
}
