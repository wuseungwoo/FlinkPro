package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
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

object MapStateKeyedUse {
  /**
    * 映射状态：MapState [K, V] 保存 Key-Value 对
    * 获取数据：MapState.get(key: K)
    * 保存数据：MapState.put(key: K, value: V)
    * 检查是否包含某个 key：MapState.contains(key: K)
    * 移除某个key对应的数据：MapState.remove(key: K)
    *
    * 使用感受：在利用map的一些特性时会用到mapState
    */
  def main(args: Array[String]): Unit = {
    /*
    去重: 去掉重复的Prod_name.
    思路: 把Prod_name作为MapState的key来实现去重, value=Transcation_amount
     */

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
    per.setProperty("group.id", "mapstateuse")

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
      .process(new MyMapStateKeyedProcessFunction)
      .map(_.map(_.product_name))
      .print()

    env.execute()

    class MyMapStateKeyedProcessFunction extends KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]] {
      //定义合适的State
      //1.ListState
      var MaxThreeState:ListState[UserTransaction] = _

      //2.MapState
      //思路: 把Prod_name作为MapState的key来实现去重, value=Transcation_amount
      var ProdNameAndTransaction:MapState[String,Long] = _

      override def open(parameters: Configuration): Unit = {
        val myListDesc:ListStateDescriptor[UserTransaction] = new ListStateDescriptor[UserTransaction]("mylistdesc",Types.of[UserTransaction])
        val myMapStateDesc: MapStateDescriptor[String, Long] = new MapStateDescriptor[String,Long]("mymapstatedesc",Types.of[String],Types.of[Long])

        //初始化
        MaxThreeState = getRuntimeContext.getListState(myListDesc)
        ProdNameAndTransaction = getRuntimeContext.getMapState(myMapStateDesc)
      }
      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]]#Context, out: Collector[ArrayBuffer[UserTransaction]]): Unit = {
        if(MaxThreeState == null){
          MaxThreeState.add(value)
          ProdNameAndTransaction.put(value.product_name,value.transaction_amount)
        }else{
          val maxThreeStateIter: util.Iterator[UserTransaction] = MaxThreeState.get().iterator()
          val maxThreeUserBuffer = new ArrayBuffer[UserTransaction]()

          while(maxThreeStateIter.hasNext){
            maxThreeUserBuffer.append(maxThreeStateIter.next())
          }

          if(ProdNameAndTransaction.contains(value.product_name)){
            if(ProdNameAndTransaction.get(value.product_name)<value.transaction_amount){

            }
          }





        }
      }
    }
  }
}
