package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util

import org.apache.flink.api.scala._
import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
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
    * 使用感受：在利用map的一些特性时会用到mapState,比如去重操作，分组取第一 等。。。
    */
  def main(args: Array[String]): Unit = {
    /*
    去重: 去掉重复的Prod_name.
    思路: 把Prod_name作为MapState的key来实现去重, value=(Prod_name,Transcation_amount)
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
        .map(_.map(
          data=>{
            (data._1._1,(data._1._2,data._2))
          }
        ))
        .map(
          mapedDatasheet=>{
            "客户Id： "+mapedDatasheet.map(_._1).toString()+" 最大的三个商品名和交易额（去重）： " + mapedDatasheet.map(_._2)
          }
        )
      .print()

    env.execute()

    //实现了每个client_id的同一个产品，只输出交易额最大的那个交易；
    //但是存在无法显示输出事对应哪个客户id的情况；——已经增加
    //还需要显示交易额：
    class MyMapStateKeyedProcessFunction extends KeyedProcessFunction[String, UserTransaction,ArrayBuffer[((String, String), Long)]] {
      var ProdNameTransactionState: MapState[String, ((String,String), Long)] = _

      override def open(parameters: Configuration): Unit = {
        val ProdNameTransactionDes = new MapStateDescriptor[String, ((String,String), Long)]("mymapstate", Types.of[String], Types.of[((String,String), Long)])

        ProdNameTransactionState = getRuntimeContext.getMapState(ProdNameTransactionDes)
      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, ArrayBuffer[((String, String), Long)]]#Context, out: Collector[ArrayBuffer[((String, String), Long)]]): Unit = {
        if (!ProdNameTransactionState.contains(value.product_name)) {
          //这里为了防止MapState的数据单调递增的情况：
          //可以增加判断：拿出当前MapState的所有transaction_amount，遍历后都大于当前需要插入的Value的transaction_amount时
          //此时不把当前Value加入MapState（目前不去实现）
          ProdNameTransactionState.put(value.product_name, ((value.client_id,value.product_name), value.transaction_amount))
        } else if (ProdNameTransactionState.contains(value.product_name)) {
          if (ProdNameTransactionState.get(value.product_name)._2 < value.transaction_amount) {
            ProdNameTransactionState.put(value.product_name, ((value.client_id,value.product_name), value.transaction_amount))
          }
        }

        val prodNameTransactionIter: util.Iterator[((String,String), Long)] = ProdNameTransactionState.values().iterator()
        val resultsArrayBuffer: ArrayBuffer[((String,String), Long)] = new ArrayBuffer[((String,String), Long)]()

        while (prodNameTransactionIter.hasNext) {
          resultsArrayBuffer.append(prodNameTransactionIter.next())
        }

        val sortedResultsArrayBuffer: ArrayBuffer[((String, String), Long)] = resultsArrayBuffer.sortWith(_._2<_._2)

        if (sortedResultsArrayBuffer.length > 3) {
          sortedResultsArrayBuffer.remove(0)
        }
//        要求输出Id需求：此时的输出-String
//        out.collect("客户Id： "+sortedResultsArrayBuffer.map(_._1._1).take(1).toString()+" 最大的三个商品交易额（去重）： "+sortedResultsArrayBuffer.map(_._1._2).toList.toString())

        //要求输出id 产品名机器对应的交易额 需求：此时的输出类型-ArrayBuffer[((String, String), Long)] /ArrayBuffer[((client_id,product_name),transaction_amount)]
        out.collect(sortedResultsArrayBuffer)
        //数据类型转换放到主分支使用map解决；
      }
    }
  }
}
