package com.seungwoo.flinkCEP.CEPCondition

import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.UserBehavior
import com.seungwoo.utils.GetKafkaStreamUtils
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream

object IterativeConditionUse {
  //  迭代条件匹配：（Iterative Condition）
  //  对前面条件匹配到的数据再次进行匹配：常写在Where()里面
  def main(args: Array[String]): Unit = {
    val kafkaStream: DataStream[String] = GetKafkaStreamUtils.getKafkaStream()
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream.map(new Gson().fromJson(_,classOf[UserBehavior]))

    val myPattern: Pattern[UserBehavior, UserBehavior] = Pattern
      .begin[UserBehavior]("start")
      .where(_.behavior == "pv")
      .or(_.behavior == "zv") //过滤出来了用户行为是pv和zv的
      .where(new IterativeCondition[UserBehavior] {
      override def filter(t: UserBehavior, context: IterativeCondition.Context[UserBehavior]): Boolean = {
        //尝试过滤下商品id>3000 的商品
        if (t.categoryID > 3000) {
          true
        } else {
          false
        }
      }
    })


    val myPatternStream: PatternStream[UserBehavior] = CEP.pattern(userBehaviorStream,myPattern)

    myPatternStream.select(new myPatternSelectFunction).print("IterativeCondition=>")

    GetKafkaStreamUtils.env.execute()
  }

  class myPatternSelectFunction extends PatternSelectFunction[UserBehavior,UserBehavior]{
    override def select(map: util.Map[String, util.List[UserBehavior]]): UserBehavior = {
      map.get("start").iterator().next()
    }
  }
}
