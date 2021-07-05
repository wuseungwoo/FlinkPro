package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.stateTTLPro

import java.util

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
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

object StateTTLUse {
  /*
  状态还可以设置 TTL（Time To Live），来限制状态的有效时间，
  状态过期后，存储的值会被自动清理，如果state是集合类型，
  那么TTL是单独针对每个元素设置的，
  也就是说每一个List元素、或者是Map的‎进入‎都有独立的TTL。

  设置TTL后，默认会在读取时自动删除，如果状态配置了backend，
  则是后台进行垃圾回收。也可以配置禁用后台垃圾回收。

  针对ListSate：
  使用感受：不如认为的按照业务要求取直接操控State内部数据的去留问题
  只是通过时间闲的有些死板

  针对MapState：
  使用感受：最好也是根据业务需求去更新状态
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
    per.setProperty("group.id", "stateTTL")

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
      .process(new UseStateTTLProcessFunction)
      .print()

    //这边需要一个ListState 让我们可以测试State 的TTL；
    //而ListState 的TTL 将对ListState中的每一个元素都将发生作用；
    class UseStateTTLProcessFunction extends KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]] {
      //这里是State如何设置TTL的操作方法:
      private val stateTtlConfig: StateTtlConfig = StateTtlConfig
        //设置状态有效时间
        .newBuilder(Time.seconds(10))
        //设置数据读写的时候是否检查过期数据
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        //设置不返回过期数据值
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build()


      //定义state和输入类型泛型一致方便处理
      var maxThreeState: ListState[UserTransaction] = _


      override def open(parameters: Configuration): Unit = {
        //初始化state
        val maxThreeStateDesc = new ListStateDescriptor[UserTransaction]("maxThreeState", Types.of[UserTransaction])

        //初始化状态描述器时，将状态TTL设置写入
        maxThreeStateDesc.enableTimeToLive(stateTtlConfig)

        maxThreeState = getRuntimeContext.getListState(maxThreeStateDesc)
      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]]#Context, out: Collector[ArrayBuffer[UserTransaction]]): Unit = {
        //针对每一个过来的元素：首先应该接入到状态中
        maxThreeState.add(value)
        //获取状态中的所有元素
        val maxThreeUserIterator: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
        //userArrayBuffer应该是def processElement 方法范围之内奏效，所以定义在此方法之外，class MyKeyedProcessFunction 之下会造成功能无法实现
        //因为每次需要一个全新的包含三个元素的ArrayBuffer作为State的全量更新的list
        //否则State中的数据会越来越多

        val userArrayBuffer: ArrayBuffer[UserTransaction] = new ArrayBuffer[UserTransaction]()

        while (maxThreeUserIterator.hasNext) {
          //          userArrayBuffer.+=(maxThreeUserIterator.next()) 错误示范：这边会让数据无法进入ArrayBuffer
          userArrayBuffer.append(maxThreeUserIterator.next())
        }

        //降序排列
        val sortedUserArrayBuffer: ArrayBuffer[UserTransaction] = userArrayBuffer.sortWith(_.transaction_amount < _.transaction_amount)
        //当长度>3时删除大于3 的部分
        if (sortedUserArrayBuffer.length > 3) {
          sortedUserArrayBuffer.remove(0)
        }
        //更新状态
        import scala.collection.JavaConverters.seqAsJavaListConverter//集合转java List
        maxThreeState.update(sortedUserArrayBuffer.toList.asJava)
        //输出结果
        out.collect(sortedUserArrayBuffer)
      }
    }
  }
}

case class UserTransaction(client_id: String, product_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long)
