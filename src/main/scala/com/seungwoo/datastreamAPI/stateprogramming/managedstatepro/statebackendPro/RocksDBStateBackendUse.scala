package com.seungwoo.datastreamAPI.stateprogramming.managedstatepro.statebackendPro

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object RocksDBStateBackendUse {
  def main(args: Array[String]): Unit = {
    // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护
    System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("hadoop.home.dir", "/usr/hdp/3.1.0.0-78/hadoop//bin/")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.设置开启检查点
    env.enableCheckpointing(5000)

    //2.针对已经开启的检查点设置检查点模式:精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //3.设置两次检查点尝试之间的最小暂停时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    //4.设置检查点超时时间
    env.getCheckpointConfig.setCheckpointTimeout(30*1000)

    //5.设置同一时间只允许一个checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //6.设置任务取消时外部保留检查点,程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //7.设置重启策略：固定延迟无限重启，改配置指定在重新启动的情况下将用于执行图的重新启动策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000))


    val stateBackend = new RocksDBStateBackend("file:///opt/flink-1.10.2/checkpoint")

    env.setStateBackend(stateBackend)
  }

}
