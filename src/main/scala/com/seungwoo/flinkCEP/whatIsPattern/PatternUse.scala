package com.seungwoo.flinkCEP.whatIsPattern

import java.util
import com.google.gson.Gson
import com.seungwoo.bean.UserBehavior
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object PatternUse {
  //Pattern入门，体会
  /**
    *体会：
    *   注意导包（如果你使用的时scala编程）
    *   patternSelectunction中是使用map去获取对应的复杂事件的定义标签key的值（标签流）
    *   思考清楚实现patternSelectFunction的输入和输出（尤其是输出）
    */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val per = new java.util.Properties()
    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","patternConsumer")

    val myFlinkKafkaConsumer = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)
    myFlinkKafkaConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFlinkKafkaConsumer)

    //=>1.创建输入事件流
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserBehavior])
      }
    )

    //=>2.定义 匹配事件 Pattern 的规则
    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.behavior == "pv")
    //=>3.将定义的 Pattern 应用在事件流上并对事件流进行检测
    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream,myPattern)
    //=>4.选取符合 Pattern 匹配到的结果
    val resultStream: DataStream[UserBehavior] = myPatternStream.select(new myPatternSelectFunction)

    resultStream.map(_.toString).print("cep=>")

    env.execute()
    
  }

  class myPatternSelectFunction extends PatternSelectFunction[UserBehavior,UserBehavior]{
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }

}
