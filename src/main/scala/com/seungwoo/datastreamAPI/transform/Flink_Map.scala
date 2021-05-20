package com.seungwoo.datastreamAPI.transform

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink_Map {
  def main(args: Array[String]): Unit = {
    //得到一个新的数据流: 新的流的元素是原来流的元素的平方
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements(1, 2, 3, 4, 5, 6)
      //匿名内部类方式实现：MapFunction[Int,Int]中前面的Int是输入参数的类型，后面的Int是输出参数的类型
      .map(new MapFunction[Int,Int] {
        override def map(t: Int):Int = {
          t*t
        }
      })

    //lambda表达式写法：
    env.fromElements(1,2,3,4,5,6).map(data=>data*data)

    //使用内部类实现
    env.fromElements(1,2,3,4,5,6).map(new MyMap).print()
    env.execute("how to use map by mapfunction")
  }

  class MyMap extends MapFunction[Int,Int]{
    override def map(t: Int): Int = {
      t*t
    }
  }
}
