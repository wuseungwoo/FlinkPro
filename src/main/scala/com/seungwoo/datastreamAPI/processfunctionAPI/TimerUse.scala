package com.seungwoo.datastreamAPI.processfunctionAPI

object TimerUse {
  /*
    Timer（定时器）是Flink Streaming API提供的用于感知并利用处理时间/事件时间变化的机制。
    官网上给出的描述如下:
    Timers are what make Flink streaming applications reactive and adaptable to processing and event time changes.
    对于普通用户来说，最常见的显式利用Timer的地方就是KeyedProcessFunction。
    我们在其processElement()方法中注册Timer，然后覆写其onTimer()方法作为Timer触发时的回调逻辑。
    根据时间特征的不同：
    1.处理时间——调用Context.timerService().registerProcessingTimeTimer()注册；onTimer()在系统时间戳达到Timer设定的时间戳时触发。
    2.事件时间——调用Context.timerService().registerEventTimeTimer()注册；onTimer()在Flink内部水印达到或超过Timer设定的时间戳时触发。
   */

  def main(args: Array[String]): Unit = {

    //参见com.seungwoo.datastreamAPI.processfunctionAPI.KeyedProcessFunctionUse
    //注意状态的初始化错误需要 重写Open方法，再Open方法里面进行运行时状态的初始化：
    /*
      //状态定义：
      //1.定义上一个状态的监控值：
      var lastTransaction: ValueState[Long] = _
      //2.定义保存定时器的时间戳：
      var currentTimer: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        //open方法中初始化：否则会报错：The runtime context has not been initialized.
        lastTransaction = getRuntimeContext.getState(new ValueStateDescriptor[Long]("LastTransaction", classOf[Long]))

        currentTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("CurrentTimer", classOf[Long]))
      }
     */
  }
}
