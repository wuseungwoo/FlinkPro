package com.seungwoo.flinkCEP.CEPCondition

import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.{ConstantClazz, UserBehavior}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SimpleConditionsUse {
  //简单条件（Simple Conditions）：其主要根据事件中的字段信息进行判断，决定是否接受该事件。
  //通过 .where() 方法对事件中的字段进行判断筛选，决定是否接受该事件
  def main(args: Array[String]): Unit = {
    //1.获取数据流
    //获取kafka流
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","SimpleCondititonsConsumer")


    val myFlinkKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](ConstantClazz.topic,new SimpleStringSchema(),per)
    myFlinkKafkaConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFlinkKafkaConsumer)

    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_,classOf[UserBehavior]))
    //2.创建Pattern（模式）
    userBehaviorStream.map(_.toString).print("主流=>")
    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.categoryID == 3000)

    //3.将Pattern应用到流上
    val patternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream,myPattern)

    //选出需要匹配到的模式流
    val resultsDataStream: DataStream[UserBehavior] = patternStream.select(new myPatternSelectFunction)

    resultsDataStream.map(_.toString).print("SimpleCondition=>")

    env.execute()
  }

  class myPatternSelectFunction extends PatternSelectFunction[UserBehavior,UserBehavior]{
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }

}
