package com.seungwoo.flinkCEP.CEPCondition

import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.UserBehavior
import com.seungwoo.utils.GetKafkaStreamUtils
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream

object StopConditionUse {
  //  终止条件（ Stop condition）：
  //  如果程序中使用了 oneOrMore 或者 oneOrMore().optional() 方法，还可以指定停止条件，
  //  否则模式中的规则会一直循环下去，如下终止条件通过 until()方法指定
  //  终止条件测试代码

  /**
    * 感受：感觉是对流的一个截取的操作，对当前的输入流进行匹配，直到匹配到了until()内的条件
    *      达到满足后就不再进行匹配；
    *      流停止；
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val kafkaStream: DataStream[String] = GetKafkaStreamUtils.getKafkaStream()
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_,classOf[UserBehavior]))

    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.behavior == "pv")
      .oneOrMore
      .until(_.categoryID > 9000)

    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream,myPattern)

    myPatternStream.select(new myPatternSelectFunction).map(_.toString).print("StopCondition=>")

    GetKafkaStreamUtils.env.execute("stopConditionExecute")
  }

  class myPatternSelectFunction extends PatternSelectFunction[UserBehavior,UserBehavior]{
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }

}
