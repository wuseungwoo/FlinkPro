package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.operatorstatepro.liststatepro

import com.google.gson.Gson
import org.apache.flink.api.scala._
import com.seungwoo.datastreamAPI.processfunctionAPI.UserTransaction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ListStateOfOperatorUse {
  def main(args: Array[String]): Unit = {
    /*
    Operator State 的数据结构：
    ListState和BroadCastState

    	列表状态（List state）
    将状态表示为一组数据的列表
     */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers","localhost:9092")
    per.setProperty("group.id","liststate")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka",new SimpleStringSchema(),per)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)

    val useraStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )
    useraStream.map(new myStateFunction)

    env.execute("how to use list state in operator state of managed state")
  }

  //接到一个生产中的需求：从队列里面接了多少条json数据（数据条数），总计数据量多少GB/TB（参考单位转换的工具类：com.seungwoo.utils.MyBytes）？
  //使用flink 在Source包下KafkaSource做出了最初版本的解决，但是表面上看着没什么问题但是深入思考还是存在一些问题的
  //思路：将json数据转换("a",1L).keyby(0).sum(1)输出；
  //可能存在的问题是：所有的数据进入到了一个key中，导致单key的数据量过大
  //可能会存在内存溢出还有因为是keyby可能会让程序并行度降低为1影响程序运行效率，
  //再仔细想想会么？。。。。。。。。。。。。。。。。效率是有影响，但是我没开窗，设置了并行度为1（避免多并行度各自计算各自的）
  //单key数据量过大，如果没有窗口的话 我是不认为他会将数据运算完了后还存在里面的，仔细查看sum(1)的源码，会发现调用了aggregate
  //aggregate属于增量聚合函数，只是会保留中间结果做更新；
  class myStateFunction extends MapFunction[UserTransaction,UserTransaction] with CheckpointedFunction {
    override def map(t: UserTransaction): UserTransaction = {

    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {

    }

    override def initializeState(context: FunctionInitializationContext): Unit = {

    }
  }


}
