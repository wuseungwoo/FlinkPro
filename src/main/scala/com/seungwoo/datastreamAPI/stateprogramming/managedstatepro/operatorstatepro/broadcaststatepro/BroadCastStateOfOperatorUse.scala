package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.operatorstatepro.broadcaststatepro

import com.google.gson.Gson
import org.apache.flink.api.scala._
import com.seungwoo.datastreamAPI.processfunctionAPI.UserTransaction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object BroadCastStateOfOperatorUse {

  //本文还用到了ProcessFunction中：BroadcastProcessFunction
  def main(args: Array[String]): Unit = {
    /*
    Broadcast State(广播状态)，
    可以将一个流中的数据广播到下游算子的每个 task中，
    使得这些数据再所有的 task 中共享，比如用于配置的一些数据，
    通过Broadcast state，我们可以让下游的每一个task，
    配置保持一致，
    当配置有修改时也能够使用Broadcast State 对下游每个 task 中的相关配置进行变更。

    Broadcast State比较适合这种场景，
    假设我们做一个规则匹配事件处理系统，
    规则是一个低吞吐的流、而事件是一个高吞吐的流。
    我们可以将规则以Broadcast State的方式发送到下游每一个任务中，
    然后当有事件需要处理时，可以从广播中读取之前的规则，进行处理。

    Broadcast State是一个Map结构，
    它比较适合于一个broadcast stream、
    另外一个不是broadcast stream的两个流进行相关操作的场景。
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //首先按照理论你需要两个流（类型可以是相同也可以不同，一般倾向于后者）！
    //所以会需要connect算子
    //而Broadcast State 底层是map

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

    //非broadcast stream：
    val useraStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )

    //broadcast stream流：此流一定是一个不太复杂的包含一定规则的流，
    //使得下游每一个每一个task保持相同的配置；
    val socketStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    //为了将需要广播的流转换成广播流
    val bCDes = new MapStateDescriptor[String, String]("BCDes", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val bCedStream: BroadcastStream[String] = socketStream.broadcast(bCDes)

    useraStream
      //双流连接,构成连接后的单流
      .connect(bCedStream)
      //process处理连接后的流
      .process(new MyBCFunction)
      .print()

    env.execute("BroadCasted Stream ")

    /**
      * 参数
      * 未广播数据类型
      * 广播数据类型
      * 输出数据类型
      */
    class MyBCFunction extends BroadcastProcessFunction[UserTransaction, String, UserTransaction] {
      override def processElement(value: UserTransaction, ctx: BroadcastProcessFunction[UserTransaction, String, UserTransaction]#ReadOnlyContext, out: Collector[UserTransaction]): Unit = {
        //通过这个方法我们可以拿到流经 非广播流（主体数据流）的每一个元素
        //那么 接下来怎么做呢？
        //实际上你有非广播流的每一个元素，flink又给了你一个map类型的算子状态，
        //你就可以用当前过来的元素和map中的value进行匹配了

        /**
          * 例如：我的这个样例类case class UserTransaction(client_id: String, client_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long)
          *
          * 我希望
          * 1.单笔交易额>10w的作为高风险客户去和map类型的BroadCastState中 key="high-risk"的value进行规则匹配或者是结合处理（获取到value+client_name）--打标记
          * 2.单笔交易额>5w <=10w的作为中等风险客户和map类型的BroadCastState中 key="medium-risk"的value进行规则匹配或者是结合处理（获取到value+client_name）--打标记
          * 3.单笔交易额<=5w 的作为常规交易客户去和map类型的BroadCastState中 key="regular-trading"的value进行规则匹配或者是结合处理（获取到value+client_name）--打标记
          * 实际生产具体情况具体对待，往往实时处理不会有太多繁琐处理降低时效性，处理计算都会后置
          */

        val transamount: Long = value.transaction_amount//交易额

        val myState: ReadOnlyBroadcastState[String, String] = ctx.getBroadcastState(bCDes)
        //拿到广播状态

        //思考：实际上用map算子，processFunction也可以做到，但是实际规则匹配如果比较复杂使用broadcaststate 可能会好点
        //上述思考需要生产实际经验去验证
        if (transamount > 100000) {
          out.collect(new UserTransaction(value.client_id,myState.get("high-risk")+value.client_name,value.transaction_amount,value.transfer_accounts,value.time))
        } else if (transamount <= 100000 && transamount > 50000) {
          out.collect(new UserTransaction(value.client_id,myState.get("medium-risk")+value.client_name,value.transaction_amount,value.transfer_accounts,value.time))
        } else {//transamount <= 5w
          out.collect(new UserTransaction(value.client_id,myState.get("regular-trading")+value.client_name,value.transaction_amount,value.transfer_accounts,value.time))
        }
      }

      override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[UserTransaction, String, UserTransaction]#Context, out: Collector[UserTransaction]): Unit = {
        //注意观察广播流进来的value：String 它代表这新过来的规则
        //既然是新规则我们需要对新规则解析和更新
        val broadCastState: BroadcastState[String, String] = ctx.getBroadcastState(bCDes)//拿到广播状态

        val regularArray: Array[String] = value.split(":")

        //按照key-value形式去更新；
        broadCastState.put(regularArray(0),regularArray(1))

      }
    }
  }
}