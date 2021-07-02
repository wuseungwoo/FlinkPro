package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object ReducingStateKeyedUse {
  /**
    * Reduce 聚合操作的状态：ReducingState[T]
    * 获取数据：ReducingState.get()
    * 添加数据：ReducingState.add(T)
    * 清空数据：ReducingState.clear()
    *
    * 使用感受：常用来统计:求和
    */
  def main(args: Array[String]): Unit = {
    //在MapStateKeyedUse中我们已经实现了：
    //要求输出id 产品名机器对应的交易额 需求：此时的输出类型-ArrayBuffer[((String, String), Long)] /ArrayBuffer[((client_id,product_name),transaction_amount)]
    //所以为了增加难度和使用ReducingState，需要实现以下需求：
    //统计每个客户对应的前三个大宗交易商品的平均交易额

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
    per.setProperty("group.id", "reducingstateuse")

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
      .process(new MyReducingStateProcessFunction)
      .print()

    env.execute()

    //统计每个客户对应的前三个大宗交易商品的交易额之和（要求商品不同）
    class MyReducingStateProcessFunction extends KeyedProcessFunction[String, UserTransaction, (String,Long)] {
      var ProdNameTransactionState: MapState[String, ((String, String), Long)] = _

      var MaxThreeTransactionSumState: ReducingState[(String, Long)] = _

      override def open(parameters: Configuration): Unit = {
        val mapStateDes = new MapStateDescriptor[String, ((String, String), Long)]("mymapstatedes", Types.of[String], Types.of[((String, String), Long)])
        ProdNameTransactionState = getRuntimeContext.getMapState(mapStateDes)


        //和其他键控状态不同的是，reducing state 需要在描述器中传入一个educeFunctionR，来告诉计入reducing state中的数据
        //以何种运算关系处理完存贮其中
        val reducingStateDes = new ReducingStateDescriptor[(String, Long)]("myrsdes", new ReduceFunction[(String, Long)] {
          override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
            (value1._1, value1._2 + value2._2)
          }
        }, Types.of[(String, Long)])
        MaxThreeTransactionSumState = getRuntimeContext.getReducingState(reducingStateDes)

      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
        if (!ProdNameTransactionState.contains(value.product_name)) {
          ProdNameTransactionState.put(value.product_name, ((value.client_id, value.product_name), value.transaction_amount))
        } else if (ProdNameTransactionState.contains(value.product_name)) {
          if(ProdNameTransactionState.get(value.product_name)._2 < value.transaction_amount){
            ProdNameTransactionState.put(value.product_name, ((value.client_id, value.product_name), value.transaction_amount))
          }
        }
        
        val resultsIter: util.Iterator[((String, String), Long)] = ProdNameTransactionState.values().iterator()
        val resultsArrayBuffer = new ArrayBuffer[((String,String),Long)]()

        while(resultsIter.hasNext){
          resultsArrayBuffer.append(resultsIter.next())
        }

        val sortedArrayBuffer: ArrayBuffer[((String, String), Long)] = resultsArrayBuffer.sortWith(_._2<_._2)

        if(sortedArrayBuffer.length > 3){
          sortedArrayBuffer.remove(0)
        }

        println(sortedArrayBuffer)

        //此时没有对ReducingState进行数据清空，过来的数据会累计在ReducingState中
        MaxThreeTransactionSumState.clear()

        for (elem <- sortedArrayBuffer) {
          MaxThreeTransactionSumState.add((elem._1._1,elem._2))
        }

        out.collect(MaxThreeTransactionSumState.get())
      }
    }
  }

}
