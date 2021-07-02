package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, MapState, MapStateDescriptor}
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

object AggregatingStateKeyedUse {
  /**
    * Aggregate 聚合操作的状态：AggregatingState [I, O]
    * 获取数据：AggregatingState.get()
    * 添加数据：AggregatingState.add(T)
    * 移除全部数据： AggregatingState.clear()
    *
    * 使用感受：可以优先考虑使用ReducingState，在ReducingState无法解决的前提下再使用AggregatingState
    */
  def main(args: Array[String]): Unit = {
    //在ReduingStateKeyedUse中我们已经实现了对每个客户最大三笔不同产品交易额的交易额之和统计
    //现在不妨尝试对这三笔交易额进行一个平均数的统计

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
      .process(new MyAggregatingStateKeyedProcessFunction)
      .print()

    env.execute()

    class MyAggregatingStateKeyedProcessFunction extends KeyedProcessFunction[String, UserTransaction, (String, Long)] {
      var ProdNameTransactionState: MapState[String, ((String, String), Long)] = _

      var MaxThreeTransactionAverageState: AggregatingState[(String, Long), (String, Long)] = _

      override def open(parameters: Configuration): Unit = {
        val mapStateDes = new MapStateDescriptor[String, ((String, String), Long)]("mymapstatedes", Types.of[String], Types.of[((String, String), Long)])
        ProdNameTransactionState = getRuntimeContext.getMapState(mapStateDes)

        // AggregatingStateDescriptor泛型：IN：输入数据类型 ACC：累加器中间数据类型 OUT：输出数据类型
        /**
          * < IN> The type of the values that are added to the state.
          * < ACC> The type of the accumulator (intermediate aggregation state).
          * < OUT> The type of the values that are returned from the state.
          */
        val myagdesc = new AggregatingStateDescriptor[(String, Long), (String, Long), (String, Long)]("myagdesc", new MyAvgFunction, Types.of[(String, Long)])
        MaxThreeTransactionAverageState = getRuntimeContext.getAggregatingState(myagdesc)
      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
        if(!ProdNameTransactionState.contains(value.product_name)){
          ProdNameTransactionState.put(value.product_name,((value.client_id,value.product_name),value.transaction_amount))
        }else if(ProdNameTransactionState.contains(value.product_name)){
          if(ProdNameTransactionState.get(value.product_name)._2 < value.transaction_amount){
            ProdNameTransactionState.put(value.product_name,((value.client_id,value.product_name),value.transaction_amount))
          }
        }

        val clientIdProdNameTransactionIter: util.Iterator[((String, String), Long)] = ProdNameTransactionState.values().iterator()
        val resultsArrayBuffer = new ArrayBuffer[((String,String),Long)]()

        while (clientIdProdNameTransactionIter.hasNext){
          resultsArrayBuffer.append(clientIdProdNameTransactionIter.next())
        }

        val sortedArrayBuffer: ArrayBuffer[((String, String), Long)] = resultsArrayBuffer.sortWith(_._2<_._2)

        if(sortedArrayBuffer.length > 3){
          sortedArrayBuffer.remove(0)
        }
        println(sortedArrayBuffer)
//        MaxThreeTransactionAverageState.clear()
//
//        for (elem <- sortedArrayBuffer) {
//          MaxThreeTransactionAverageState.add(elem._1._1,elem._2)
//        }
//
//        out.collect(MaxThreeTransactionAverageState.get())
        var avgCounts:Long  = 0L
        var clientId:String = " "
        val dataCount: Int = sortedArrayBuffer.length
        for (elem <- sortedArrayBuffer) {
          clientId = elem._1._1
          avgCounts = avgCounts + elem._2
        }

        //吊诡的是实现了需求，不需要用到AggregatingState，实现复杂，不建议使用；
        //相比较使用较丰富的功能类，简洁易懂高效的代码更为重要
        //结论 优先使用ReducingState
        out.collect(clientId,avgCounts/dataCount)

      }

      class MyAvgFunction extends AggregateFunction[(String, Long), (String, Long), (String, Long)] {
        //定义数据个数
        var dataCounts:Int = _
        override def add(value: (String, Long), accumulator: (String, Long)): (String, Long) = {
          dataCounts = dataCounts + 1
          if(dataCounts > 3){
            dataCounts = 3
          }
          (value._1,(value._2+accumulator._2)/dataCounts)
        }

        override def createAccumulator(): (String, Long) = {
          dataCounts = 0
          ("", 0L)
        }

        override def getResult(accumulator: (String, Long)): (String, Long) = {
          (accumulator._1,accumulator._2)
        }

        override def merge(a: (String, Long), b: (String, Long)): (String, Long) = {
          (a._1,a._2+b._2)
        }
      }
    }
  }
}
