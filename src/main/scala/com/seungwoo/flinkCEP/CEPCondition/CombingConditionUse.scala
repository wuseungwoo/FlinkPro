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

object CombingConditionUse {
  //  组合条件（Combining Conditions）：是将简单条件进行合并
  //  .where() 方法进行条件的组合，表示 AND
  //  .or() 方法进行条件的组合，表示 OR
  def main(args: Array[String]): Unit = {
    //1.获取流
    val kafkaStream: DataStream[String] = GetKafkaStreamUtils.getKafkaStream()
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_, classOf[UserBehavior]))
    //2.获取Pattern
    //尝试匹配用户行为是pv且产品id=3000 或用户行为是uv且产品id=4000 的用户数据
    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.behavior == "pv")
      .where(_.categoryID == 3000)
      .or(_.behavior == "uv")
      .where(_.categoryID == 4000)
    //3.Pattern应用到流
    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream, myPattern)
    //4.选择标签流
    myPatternStream.select(new myPatternselectFunction).print("CombingCondition=>")

    GetKafkaStreamUtils.env.execute()
  }

  class myPatternselectFunction extends PatternSelectFunction[UserBehavior, UserBehavior] {
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }


}
