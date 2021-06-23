package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util

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

import scala.collection.mutable.ListBuffer

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
      //定义合适的状态:用来存储client_id？还是装UserTransaction？这样的话需要的是client_id比较的却是transaction_amount
      var maxThreeState: ListState[UserTransaction] = _

      //需要一个统计结果来统计maxThreeState中的client_id的个数是否达到3个
      var count:Int = _

      //需要一个ListBuffer[String]来装载client_id做输出
      var maxThreeClientIDListBuffer:ListBuffer[String] = _
      //定义装载三个long数据的List[long](为了方便处理数据)：用来存储交易额
      var maxThreeTransactionAmountListBuffer:ListBuffer[Long] = _
      //初始化状态
      override def open(parameters: Configuration): Unit = {
        val maxthreeliststatedesc: ListStateDescriptor[UserTransaction] = new ListStateDescriptor[UserTransaction]("maxthreeliststate",Types.of[UserTransaction])
        maxThreeState = getRuntimeContext.getListState(maxthreeliststatedesc)
        count = 0
        maxThreeClientIDListBuffer = new ListBuffer[String]
        maxThreeTransactionAmountListBuffer = new ListBuffer[Long]
      }
      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, List[String]]#Context, out: Collector[List[String]]): Unit = {
        //如何保证当前list中始终为目前处理的元素中最大的三位？
        //最终目的是想让maxThreeState 里面始终有三位当前最大的数
        //为了达到这个目的我借助了ListBuffer[long]的特性做处理

        //首先我需要判断maxThreeStatede的数字个数是否<3
        val maxThreestateIterator: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
        while (maxThreestateIterator.hasNext){
          count = count + 1
        }

        if(count<=3){
          //说明进来的元素并没有把人为设定的size大小为3的maxThreeState装满
          //那么每过来一个元素也不需要借用maxThreeListBuffer来比较数字大小，请直接进入maxThreeState
          maxThreeState.add(value)
          //输出需要的client_id
          val stateLessThree: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
          while(stateLessThree.hasNext){
            maxThreeClientIDListBuffer.append(stateLessThree.next().client_id)
            maxThreeTransactionAmountListBuffer.append(stateLessThree.next().transfer_accounts)
          }
          out.collect(maxThreeClientIDListBuffer.toList)
        }else{
          //此时maxThreeState内部已经拥有三个元素
          //当第四个进来，我们就需要比较和移除元素了
          val minTransactionAmount: Long = maxThreeTransactionAmountListBuffer.min
          if(value.transaction_amount > minTransactionAmount){
            //移除装载最大交易额三个中最小的一个交易额
            maxThreeTransactionAmountListBuffer.-=(minTransactionAmount)
            maxThreeTransactionAmountListBuffer.append(value.transaction_amount)

            //移除装在最大交易额对应的client_id中最小交易额对应的client_id
            while(maxThreestateIterator.hasNext){
              if(maxThreestateIterator.next().transaction_amount == minTransactionAmount){
                maxThreeClientIDListBuffer.-=(maxThreestateIterator.next().client_id)
              }
            }
            maxThreeClientIDListBuffer.append(value.client_id)
            out.collect(maxThreeClientIDListBuffer.toList)
          }
        }
      }
    }
  }
}
