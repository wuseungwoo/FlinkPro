package com.seungwoo.datastreamAPI.transform

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object Flink_FlatMap {
  def main(args: Array[String]): Unit = {
    //扁平映射，stream->stream
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //使用匿名内部类实现
    //扁平化会有个collector来收集，最后通过collect发出；
    //从这里看出来匿名内部类的优势在于自动推导，后续也将大量只使用匿名内部类的方式来实现
    env.fromElements(1,2,3,4,5,6)
      .flatMap(new FlatMapFunction[Int,Int] {
        override def flatMap(t: Int, collector: Collector[Int]) = {
          collector.collect(t*t)
          collector.collect(t*2)
        }
      })

    //使用lambda表达式实现
    env.fromElements(1,2,3,4,5,6).flatMap((data:Int,coll:Collector[Int])=>{
      coll.collect(data*data)
      coll.collect(data*2)
    }).print()

    //也可使用静态内部类
    //不在赘述

    env.execute("what is flatmap")
  }

}
