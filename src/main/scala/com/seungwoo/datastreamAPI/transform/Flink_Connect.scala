package com.seungwoo.datastreamAPI.transform

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, CoProcessFunction}
import org.apache.flink.util.Collector

object Flink_Connect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //只是机械性的将两个流捏合在一起，内部仍然是分离的2个流，只能2个流进行connect, 不能有第3个参与
    env.setParallelism(1)

    val intStream: DataStream[Int] = env.fromElements(1,2,3,4,5)

    val strStream: DataStream[String] = env.fromElements("a","b","c","d","e")

    val conStream: ConnectedStreams[Int, String] = intStream.connect(strStream)


    //CoMapFunction[Int,String,String]前两个为输入类型，最后一个为输出参数,取上游参数类型，否则类转换异常
    conStream.map(new CoMapFunction[Int,String,String] {
      override def map2(in2: String) = {
        in2
      }

      override def map1(in1: Int) = {
        in1.toString
      }
    }).print()
    //这里测试只输出一个流
    /*
    之前方式：
    ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
    cs.getFirstInput().print("first");
    cs.getSecondInput().print("second");
     */
    //现在使用
    conStream.process(new CoProcessFunction[Int,String,Unit] {
      override def processElement1(value: Int, ctx: CoProcessFunction[Int, String, Unit]#Context, out: Collector[Unit]): Unit = {
        out.collect(value)
    }

      override def processElement2(value: String, ctx: CoProcessFunction[Int, String, Unit]#Context, out: Collector[Unit]): Unit = {
        //这里不做输出
      }

    }).print("单流输出")

    env.execute("what`s connect")
  }

}
