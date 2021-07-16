package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.statebackendPro

import com.google.gson.Gson
import com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.stateTTLPro.UserTransaction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FlinkExactlyOnceCodeTemplate {
  //Flink精确一次性代码范本参考

  //Flink精确一次性原理：
  /**
    * 	内部 —— 利用checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证部的状态一致性
    * 	source —— kafka consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性
    * 	sink —— kafka producer作为sink，采用两阶段提交 sink，需要实现一个 TwoPhaseCommitSinkFunction
    *
    * Flink状态后端选择依据：
    *
    * FsStateBackend：
    * 状态比较大、窗口比较长、key/value 状态比较大的 Job。
    * 所有高可用的场景。
    *
    * RocksDBStateBackend：
    * 状态非常大、窗口非常长、key/value 状态非常大的 Job。
    * 所有高可用的场景。
    */

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //一：利用checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证部的状态一致性:
    // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护
    System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("hadoop.home.dir", "/usr/hdp/3.1.0.0-78/hadoop//bin/")

    //1.开启定时检查点
    env.enableCheckpointing(5000)

    //2.针对已经开启的检查点设置精确一次性
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //3.设置两次检查点尝试之间的最小暂停时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    //4.设置检查点超时时间
    env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

    //5.设置同一时间只允许一个checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //6.设置任务取消时外部保留检查点,程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //7.设置重启策略：固定延迟无限重启，改配置指定在重新启动的情况下将用于执行图的重新启动策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000))

    //创建全量RocksDB状态后端
    val rocksDBStateBackend = new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints/mssql/realtime/realtimetest")
    env.setStateBackend(rocksDBStateBackend)


    //二： kafka consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性
    val consumerProperties = new java.util.Properties()
    consumerProperties.setProperty("bootstrap.servers", "localhost:9092")
    consumerProperties.setProperty("group.id", "exactlyOnceUse")
    /*
    控制如何读取以事务方式编写的消息。
    如果设置为读取已提交的，那么consumer.poll()将只返回已提交的事务性消息。
    如果设置为read uncommitted'(缺省值)，consumer.poll()将返回所有消息，甚至是已经中止的事务性消息。
    非事务性消息将以两种模式无条件返回。
    消息将总是按偏移顺序返回。
    因此，在读提交模式中，消费器.poll()将只返回到最后一个稳定偏移量(LSO)的消息，LSO小于第一个打开的transac的偏移量
     */
    consumerProperties.setProperty("isolation.level", "read_committed")
    /*
    服务器应该为获取请求返回的最小数据量。
    如果可用数据不足，请求将等待大量数据累积，然后再响应请求。
    1字节的默认设置意味着获取请求被回答，当一个数据的单个字节可用或获取请求超时等待数据到达。
    将这个值设置为大于1的值将导致服务器等待更大数量的数据积累，这会在增加一些延迟的代价下提高服务器吞吐量。
     */
    consumerProperties.setProperty("fetch.min.bytes", "10")
    val kafkaConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), consumerProperties)

    kafkaConsumer.setStartFromLatest() //从失败前提交到CeckPoint的最后一次提交的位置开始消费


    //kafkaConsumer.setCommitOffsetsOnCheckpoints(true)//默认为True
    /**
      * 启用checkpoint：当执行checkpoint时，Flink Kafka Consumer将offset提交并存储在checkpoint中。
      * 这可以确保Kafka broker中提交的偏移量与检查点状态中的偏移量一致。
      * 用户也可以通过Consumer的setCommitOffsetsOnCheckpoints(boolean)方法来禁用或启用偏移提交(默认情况下为true)。
      * 注意，这种场景下在Kafka Consumer属性中定义的自动提交offset的配置则会完全被忽略。
      */

    val kafkaStream: DataStream[String] = env.addSource(kafkaConsumer)

    val userStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        new Gson().fromJson(data, classOf[UserTransaction])
      }
    )

    val resultStream: DataStream[String] = userStream.map(
      data => {
        new Gson().toJson(data.client_id + data.transaction_amount)
      }
    )


    //三：kafka producer作为sink，采用两阶段提交 sink，需要实现一个 TwoPhaseCommitSinkFunction
    //kafka sink 保证exactly-once
    //https://www.cnblogs.com/wangzhuxing/p/10125437.html#_label3_1
    val producerProperties = new java.util.Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")
    /*
    生产者要求领导者在确认请求完成之前已收到的确认数。 这控制了发送记录的持久性。 允许以下设置：
    acks = 0如果设置为零，那么生产者将完全不等待服务器的任何确认。 该记录将立即添加到套接字缓冲区中并视为已发送。 在这种情况下，不能保证服务器已收到记录，并且重试配置不会生效（因为客户端通常不会知道任何故障）。 为每个记录提供的偏移量将始终设置为-1。
    acks = 1这意味着领导者会将记录写入其本地日志，但会在不等待所有关注者的完全确认的情况下做出响应。 在这种情况下，如果领导者在确认记录后立即失败，但是在跟随者复制记录之前，则记录将丢失。
    acks = all这意味着领导者将等待完整的同步副本集确认记录。 这样可以确保只要至少一个同步副本仍处于活动状态，记录就不会丢失。 这是最有力的保证。 这等效于acks = -1设置。
     */
    //设置ack=-1
    producerProperties.setProperty("acks", "all")

    //设置失败重试次数
    producerProperties.setProperty("retries", "3")

    /**
      * 启用checkpoint：当执行checkpoint时，
      * Flink Kafka Consumer将offset提交并存储在checkpoint中。
      * 这可以确保Kafka broker中提交的偏移量与检查点状态中的偏移量一致。
      * 用户也可以通过Consumer的setCommitOffsetsOnCheckpoints(boolean)
      * 方法来禁用或启用偏移提交(默认情况下为true)。
      * 注意，这种场景下在Kafka Consumer属性中定义的自动提交offset的配置则会完全被忽略。
      */
    producerProperties.setProperty("transaction.timeout.ms", "600000")
    producerProperties.setProperty("enable.idempotence", "true") // 设置幂等性

    val myProducer = new FlinkKafkaProducer[String](
      "my-topic",               // 目标 topic
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), // 序列化 schema
      producerProperties,               // producer 配置
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE) //
    resultStream.addSink(myProducer)

    env.execute()
  }
}
