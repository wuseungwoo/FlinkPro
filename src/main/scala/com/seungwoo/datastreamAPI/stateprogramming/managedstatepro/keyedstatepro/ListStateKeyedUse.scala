package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream,StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

object ListStateKeyedUse {
  //flink管理的状态之键控状态的ListState
  //注意：时刻在心里提醒自己监控状态都是key级别的粒度，同一个key公用一个其对应的状态，且和并行度和任务数无关

  /**
    * ListState
    *1.列表状态：ListState [T] 保存一个列表，列表里的元素的数据类型为 T
    * *
    *2.获取列表状态：ListState.get() 返回 Iterable[T]
    * *
    *3.添加单个元素到列表状态：ListState.add(value: T)
    * *
    *4.添加多个元素到列表状态：ListState.addAll(values: java.util.List[T])
    * *
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

    //针对每个用户交易输出交易额最高的3个product_name
    //测试数据范例：
    /**
      * {"client_id":"1001","product_name":"prod_01","transaction_amount":50000,"transfer_accounts":50000,"time":1624501616643}
      */
    //始终将当前进入的数据中最高的三个保持输出
    //数据流一定是keyedStream然后Process

    useraStream
      .keyBy(_.client_id)
      .process(new MyKeyedProcessFunction)
      //改变数据输出类型请放在Process算子之后
      .map(_.map(_.product_name))
      .print("分组状态流：")

    env.execute("Flink managed keyed state")


    //保证输入类型和输出类型的泛型一致方便处理
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]] {
      //定义state和输入类型泛型一致方便处理
      var maxThreeState: ListState[UserTransaction] = _

      override def open(parameters: Configuration): Unit = {
        //初始化state
        val maxThreeStateDesc = new ListStateDescriptor[UserTransaction]("maxThreeState", Types.of[UserTransaction])

        maxThreeState = getRuntimeContext.getListState(maxThreeStateDesc)
      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]]#Context, out: Collector[ArrayBuffer[UserTransaction]]): Unit = {
        //针对每一个过来的元素：首先应该接入到状态中
        maxThreeState.add(value)
        //获取状态中的所有元素
        val maxThreeUserIterator: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
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
        import scala.collection.JavaConverters.seqAsJavaListConverter
        maxThreeState.update(sortedUserArrayBuffer.toList.asJava)
        //输出结果
        out.collect(sortedUserArrayBuffer)
      }
    }
  }
}

case class UserTransaction(client_id: String, product_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long) {
}
