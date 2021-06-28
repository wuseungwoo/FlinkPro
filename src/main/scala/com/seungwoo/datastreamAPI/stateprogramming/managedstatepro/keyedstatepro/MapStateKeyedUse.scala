package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.keyedstatepro

import java.util
import java.util.Comparator

import com.google.gson.Gson
import org.apache.flink.api.scala._

import scala.collection.JavaConverters.seqAsJavaListConverter
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

import scala.collection.mutable.ArrayBuffer

object MapStateKeyedUse {
  //flink管理的状态之键控状态的MapState key-value 形式的状态
  //注意：时刻在心里提醒自己监控状态都是key级别的粒度，同一个key公用一个其对应的状态，且和并行度和任务数无关

  //只能作用于KeyedStream上：
  /**
    *映射状态：MapState [K, V] 保存 Key-Value 对

    *获取数据：MapState.get(key: K)

    *保存数据：MapState.put(key: K, value: V)

    *检查是否包含某个 key：MapState.contains(key: K)

    *移除某个key对应的数据：MapState.remove(key: K)
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

    //实际上这里存在product_name重复的情况，因为交易额最高的三笔交易对应的Product_name可能是一样的；

    //当下Flink的去重方法总结：
    /*
    1.MapState 方式去重
    2.SQL 方式去重
    3.HyperLogLog 方式去重
    4.Bitmap 精确去重

    5.hashSet
    Flink处理日均亿级别及以上的日志数据为背景，讨论除了朴素方法（HashSet）之外的三种实时去重方案，即：
    6.布隆过滤器
    7.RocksDB状态后端
    8.外部存储
     */

    //数据量可能是选择哪种去重方案的一个重要因素：
    //仅以本文的案列为基础情况进行分析：
    //本书数据量是非常的小，只需要对最多四个元素的ArrayBuffer的数据进行去重操作；
    //因此可选去重方案有：
    /*
    1.MapState
    2.HashSet
     */


    useraStream
      .keyBy(_.client_id)
      .process(new MyProcessFunction)
      .print("去重后的流: ")

    env.execute("去重后的流： ")

    class  MyProcessFunction extends KeyedProcessFunction[String,UserTransaction,ArrayBuffer[UserTransaction]]{
      //定义装载交易额最大的三条[UserTransaction]的State
      var maxThreeState:ListState[UserTransaction] = _

      //定义合适的数据处理
      override def open(parameters: Configuration): Unit = {
        //初始化state
        val maxThreeStateDesc = new ListStateDescriptor[UserTransaction]("maxThreeState", Types.of[UserTransaction])
        maxThreeState = getRuntimeContext.getListState(maxThreeStateDesc)
      }

      override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, ArrayBuffer[UserTransaction]]#Context, out: Collector[ArrayBuffer[UserTransaction]]): Unit = {
        //本模块使用HashSet的方法来进行去重操作
        /*
        假定数据：A:5000 A:6000 A:7000
        保留数据：A:7000
         */
        //针对每一个过来的元素：首先应该先对当前全员的数据进行去重:当product_name 相同的数据保留transaction_amount大的那条数据
//        val firstStateUpdate:ArrayBuffer[UserTransaction] = new ArrayBuffer[UserTransaction]()
//        //获取value加入之前的状态内部数据
//        val addValueBeforStataData: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
//
//
//        while(addValueBeforStataData.hasNext){
//          firstStateUpdate.append(addValueBeforStataData.next())
//        }
//
//        println("一开始从状态中拿到的 "+firstStateUpdate)
//
//
//
//        if(firstStateUpdate.length == 0){
//          maxThreeState.add(value)
//        }else{
//          while(addValueBeforStataData.hasNext){
//            //把状态内部的每个数据拿出来和进入的Value进行比对
//            if(value.product_name != addValueBeforStataData.next().product_name){//发现不同加入state
//              maxThreeState.add(value)
//            }else{//相同需要判断 transaction_amount 的值的大小
//              if(addValueBeforStataData.next().transaction_amount < value.transaction_amount){
//                //更新State
//
//                for (elem <- firstStateUpdate) {
//                  if(elem.product_name == value.product_name){
//                    firstStateUpdate.-=(elem)
//                  }
//                }
//
//                println("去除元素后的："+firstStateUpdate)
//
//                maxThreeState.update(firstStateUpdate.toList.asJava)
//                maxThreeState.add(value)
//              }
//            }
//          }
//        }



        maxThreeState.add(value)
        //获取状态中的所有元素
        val maxThreeUserIterator: util.Iterator[UserTransaction] = maxThreeState.get().iterator()
        val userArrayBuffer: ArrayBuffer[UserTransaction] = new ArrayBuffer[UserTransaction]()

        //用removeDuplicationUserArrayBuffer的元素 去除 userArrayBuffer的重复元素
        val removeDuplicationUserArrayBuffer  = new ArrayBuffer[UserTransaction]()


        while (maxThreeUserIterator.hasNext) {
          removeDuplicationUserArrayBuffer.append(maxThreeUserIterator.next())
          userArrayBuffer.append(maxThreeUserIterator.next())
        }

        //useArrayBuffer取出同商品ID中价格最高的一个
        for (removeUser <- removeDuplicationUserArrayBuffer) {
          for (user <- userArrayBuffer) {
            if(removeUser.product_name == user.product_name && user.transaction_amount<removeUser.transaction_amount){
              userArrayBuffer.-=(user)
            }
          }
        }
        //降序排列
        val sortedUserArrayBuffer: ArrayBuffer[UserTransaction] = userArrayBuffer.sortWith(_.transaction_amount < _.transaction_amount)

        //当长度>3时删除大于3 的部分
        if (sortedUserArrayBuffer.length > 3) {
          sortedUserArrayBuffer.remove(0)
        }
        //更新状态
        maxThreeState.update(sortedUserArrayBuffer.toList.asJava)
        //输出结果
        out.collect(sortedUserArrayBuffer)
      }
    }
  }
}