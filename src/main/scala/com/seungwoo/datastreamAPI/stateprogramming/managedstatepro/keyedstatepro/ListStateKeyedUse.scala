package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.datastreamAPI.processfunctionAPI.UserTransaction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object ListStateKeyedUse {
  //flink管理的状态之键控状态的ListState
  //注意：时刻在心里提醒自己监控状态都是key级别的粒度，同一个key公用一个其对应的状态，且和并行度和任务数无关

  /**
    * ListState
    *1.列表状态：ListState [T] 保存一个列表，列表里的元素的数据类型为 T
    **
    *2.获取列表状态：ListState.get() 返回 Iterable[T]
    **
    *3.添加单个元素到列表状态：ListState.add(value: T)
    **
    *4.添加多个元素到列表状态：ListState.addAll(values: java.util.List[T])
    **
 *5.添加多个元素更新列表状态的数据：ListState.update(values: java.util.List[T])
    */
  def main(args: Array[String]): Unit = {
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
    per.setProperty("group.id", "liststate")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)

    val useraStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )

    //针对每个用户交易输出交易额最高的3个用户id/范例：3个用户id:String组成一个
    //始终将当前进入的数据中最高的三个保持输出

    //数据流一定是keyedStream然后Process

    useraStream
      .keyBy(_.client_id)
      .process(new MyKeyedProcessFunction)
      .print()

    env.execute("Flink managed keyed state")


    class MyKeyedProcessFunction extends KeyedProcessFunction[String, UserTransaction, List[String]] {
      //定义合适的状态
      var maxThreeState: ListState[Long] = _

      //定义装在三个long数据的iterator(为了方便处理数据)
      var maxThreeList:List[Long] = _
      //初始化状态
      override def open(parameters: Configuration): Unit = {
        val maxthreeliststatedesc: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("maxthreeliststate",Types.of[Long])
        maxThreeState = getRuntimeContext.getListState(maxthreeliststatedesc)

        maxThreeList = List()
      }
      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, List[String]]#Context, out: Collector[List[String]]): Unit = {
        //如何保证当前list中始终为目前处理的元素中最大的三位？
        //每次都要遍历一遍的话会不会很耗性能？但是这个list中确只有三个元素...


      }
    }
  }
}
