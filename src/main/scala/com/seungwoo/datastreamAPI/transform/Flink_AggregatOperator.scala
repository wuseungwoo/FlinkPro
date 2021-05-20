package com.seungwoo.datastreamAPI.transform

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink_AggregatOperator {
  def main(args: Array[String]): Unit = {
    //sum,max,min,maxBy,minBy
    /*
    KeyedStream的每一个支流做聚合。
    执行完成后，会将聚合的结果合成一个流返回，所以结果都是DataStream
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(1,2,3,4,5,6,7,8).keyBy(
      data=>{
        if(data % 2 == 0){
          "偶数"
        }
      }
    )
  }
}
