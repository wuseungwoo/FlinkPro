package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.operatorstatepro.liststatepro

import java.util

import com.google.gson.Gson
import org.apache.flink.api.scala._
import com.seungwoo.datastreamAPI.processfunctionAPI.UserTransaction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, TypeInformation}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ListStateOfOperatorUse {
  def main(args: Array[String]): Unit = {
    /*
    Operator State 的数据结构：
    ListState和BroadCastState

    	列表状态（List state）
    将状态表示为一组数据的列表
     */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(5000)
    // 使用文件存储的状态后端
    val stateBackend = new FsStateBackend("file:///opt/flink/checkpoint",true)
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

    useraStream.map(new myStateFunction)

    env.execute("how to use list state in operator state of managed state")
  }

  //接到一个生产中的需求：从队列里面接了多少条json数据（数据条数），总计数据量多少GB/TB（参考单位转换的工具类：com.seungwoo.utils.MyBytes）？
  //使用flink 在Source包下KafkaSource做出了最初版本的解决，但是表面上看着没什么问题但是深入思考还是存在一些问题的
  //思路：将json数据转换("a",1L).keyby(0).sum(1)输出；
  //可能存在的问题是：所有的数据进入到了一个key中，导致单key的数据量过大
  //可能会存在内存溢出还有因为是keyby可能会让程序并行度降低为1影响程序运行效率，
  //再仔细想想会么？。。。。。。。。。。。。。。。。效率是有影响，但是我没开窗，设置了并行度为1（避免多并行度各自计算各自的）
  //单key数据量过大，如果没有窗口的话 我是不认为他会将数据运算完了后还存在里面的，仔细查看sum(1)的源码，会发现调用了aggregate
  //aggregate属于增量聚合函数，只是会保留中间结果做更新；
  class myStateFunction extends MapFunction[UserTransaction, Long] with CheckpointedFunction {
    //定义计算结果统计变量
    var counts: Long = _

    //定义算子状态变量：因为是算子状态所以只有两种ListState / BroadcastState
    var myListState: ListState[Long] = _

    //初始化定义的变量
    override def initializeState(context: FunctionInitializationContext): Unit = {
      //counts变量初始化
      counts = 0L
      val myListStateDescriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("MyListState",TypeInformation.of(classOf[Long]))

      //算子状态list state状态初始化
      myListState = context.getOperatorStateStore.getListState(myListStateDescriptor)
      val listStateIterator: util.Iterator[Long] = myListState.get().iterator()
      while (listStateIterator.hasNext) {
        counts = counts + listStateIterator.next()
      }

      //程序重启时恢复counts
      if(context.isRestored){
        val resoredListState: util.Iterator[Long] = myListState.get().iterator()

        while(resoredListState.hasNext){
          counts = counts + resoredListState.next()
        }
      }

    }

    //对于过来的每一个元素你想执行什么操作
    override def map(t: UserTransaction): Long = {
      counts = counts + 1L
      counts
    }

    //持久化状态时做快照：快照状态
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      myListState.clear()
      myListState.add(counts)
    }

  }


}
