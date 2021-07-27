package com.seungwoo.flinkCEP.CEPContraint

import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.UserBehavior
import com.seungwoo.utils.GetKafkaStreamUtils
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

object TimeContraint {
  //可以为模式指定时间约束，用来要求在多长时间内匹配有效
  def main(args: Array[String]): Unit = {
    //当一个模式上通过within加上窗口长度后
    //部分匹配的事件序列就可能因为超过窗口长度而被丢弃。

    val kafkaStream: DataStream[String] = GetKafkaStreamUtils.getKafkaStream()
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_, classOf[UserBehavior]))

    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern.begin[UserBehavior]("start")
      .where(_.behavior == "pv")
      .where(_.categoryID == 3000)
      .within(Time.seconds(5))
    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream, myPattern)

    myPatternStream.select(new MyPatternSelectFunction).map(_.toString).print("WithinWindowingTime=>")

    GetKafkaStreamUtils.env.execute("WithinWindowingTimeExecute")
  }

  class MyPatternSelectFunction extends PatternSelectFunction[UserBehavior, UserBehavior] {
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }

}
