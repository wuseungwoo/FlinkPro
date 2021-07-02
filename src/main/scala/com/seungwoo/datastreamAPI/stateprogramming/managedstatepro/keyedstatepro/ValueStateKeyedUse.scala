package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ValueStateKeyedUse {
  /**
    * 单值状态：ValueState [T] 保存单个的值，值的类型为 T
    * 获取状态值：ValueState.value()
    * 更新状态值：ValueState.update(value: T)
    *
    * 使用感受：一般是对两个值的关系做处理的时候会偏向于使用算子状态ValueState，状态中存贮一个单值的时候
    * 注意：只适用Keyed stream
    */

  def main(args: Array[String]): Unit = {
    //如果一个客户的相邻两笔交易的差额大于10w便发生报警:输出对应的client_id

    //获取kafkaStream
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(5000)
    // 使用文件存储的状态后端
    val stateBackend = new FsStateBackend("file:///opt/flink/checkpoint", true)
    env.setStateBackend(stateBackend)
    // 设置检查点模式（精确一次 或 至少一次）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次检查点尝试之间的最小暂停时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 设置检查点超时时间
    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)
    // 设置可能同时进行的最大检查点尝试次数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 使检查点可以在外部保留
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "valuestate")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)

    val useraStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )

    useraStream
      .keyBy(_.client_id)
      .process(new MyValueStateProcessFunction)
      .print()

    env.execute()

    class MyValueStateProcessFunction extends KeyedProcessFunction[String,UserTransaction,String]{
      //定义ValueState
      //存储上一笔交易的交易额
      var preTransacation:ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        val myValueStateDes = new ValueStateDescriptor[Long]("MyValueState",Types.of[Long])
         preTransacation = getRuntimeContext.getState(myValueStateDes)
      }
      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, String]#Context, out: Collector[String]): Unit = {
        if(preTransacation.value() == null){
          preTransacation.update(value.transaction_amount)
        }else{
          if(Math.abs(value.transaction_amount - preTransacation.value()) > 100000L){
            preTransacation.update(value.transaction_amount)
            out.collect(value.client_id)
          }
        }
      }
    }
  }

}
