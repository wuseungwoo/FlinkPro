package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.statebackendPro

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FsStateBackendUse {
  /*
  FsStateBackend 将正在运行中的状态数据保存在 TaskManager 的内存中。
  CheckPoint 时，将状态快照写入到配置的文件系统目录中，
  通常是复制的高可用性文件系统，例如HDFS ， Ceph ， S3 ， GCS 等。
  少量的元数据信息存储到 JobManager 的内存中（高可用模式下，
  将其写入到 CheckPoint 的元数据文件中）。

  file system state backend 有着内存级别的快速访问和文件系统存储的安全性
  能够更好的容错

  file system state backend 需要配置一个文件系统的 URL（类型、地址、路径）
  例如：”hdfs://namenode:40010/flink/checkpoints”
  或 “file:///data/flink/checkpoints”。

  FsStateBackend 默认使用异步快照来防止 CheckPoint
  写状态时对数据处理造成阻塞。 用户可以在实例化 FsStateBackend 的时候，
  将相应布尔类型的构造参数设置为 false 来关闭异步快照

  使用场景：
  状态比较大、窗口比较长、key/value 状态比较大的 Job。
  所有高可用的场景。
   */
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //设置开启检查点
    env.enableCheckpointing(5000)

    //我想使用FsStateBackend
    val myFsStateBackend = new FsStateBackend("hdfs://nameservice1/flink/checkpoints/mssql/realtime/realtimetest",true)
    env.setStateBackend(myFsStateBackend)

    //针对已经开启的检查点设置检查点模式:精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //设置两次检查点尝试之间的最小暂停时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    //设置检查点超时时间
    env.getCheckpointConfig.setCheckpointTimeout(30*1000)

    //设置可能同时进行的最大检查点尝试次数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //设置任务取消时外部保留检查点
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略：固定延迟无限重启，改配置指定在重新启动的情况下将用于执行图的重新启动策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000))


  }
}
