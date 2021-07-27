package com.seungwoo.flinkCEP.CEPCirculation

import java.util
import com.google.gson.Gson
import org.apache.flink.api.scala._
import com.seungwoo.bean.UserBehavior
import com.seungwoo.utils.GetKafkaStreamUtils
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream

object CirculationTimesUse {
  //对于已经创建好的 Pattern，可以在一个简单条件后追加量词，
  //也就是指定循环次数，形成循环执行的 Pattern

  /**
    * //指定循环触发4次
    * start.times(3)
    * //可以执行触发次数范围,让循环执行次数在该范围之内
    * start.times(2, 4)
    */

  def main(args: Array[String]): Unit = {
    val kafkaStream: DataStream[String] = GetKafkaStreamUtils.getKafkaStream()
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_, classOf[UserBehavior]))

    userBehaviorStream.print()
    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.behavior == "pv")
      .where(_.categoryID == 3000).times(2)
      .next("next").where(_.categoryID == 5000)

    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream,myPattern)

    myPatternStream.select(new myPatternSelectFunction).print("circulationTimers=>")

    GetKafkaStreamUtils.env.execute()
  }

  class myPatternSelectFunction extends PatternSelectFunction[UserBehavior,String]{
    override def select(map: util.Map[String, util.List[UserBehavior]]): String = {
      val startUserBehavior: UserBehavior = map.get("start").iterator().next()
      val nextBehavior: UserBehavior = map.get("next").iterator().next()

      val results: String = startUserBehavior.toString+nextBehavior.toString
      return results
    }
  }

}
