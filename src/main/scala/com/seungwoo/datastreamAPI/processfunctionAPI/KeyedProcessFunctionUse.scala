package com.seungwoo.datastreamAPI.processfunctionAPI


import java.time.Duration

import com.google.gson.Gson
import org.apache.flink.api.scala._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object KeyedProcessFunctionUse {
  def main(args: Array[String]): Unit = {
    /*KeyedProcessFunction用来操作KeyedStream，
  KeyedProcessFunction会处理流的每一个元素（每条数据来了之后都可以处理、过程处理函数），输出为0个、1个或者多个元素。
  所有的ProcessFunction都继承自RichFunction接口
  富函数，它可以有各种生命周期、状态的一些操作，获取watermark、定义闹钟定义定时器等
  所以都有open()、close()和getRuntimeContext() 等方法。而KeyedProcessFunction[KEY, IN, OUT] 还额外提供了两个方法：
  1.processElement(I value, Context ctx, Collector<O> OUt)流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
  Context可以访问元素的时间戳，元素的key，以及TimerService时间服务。
  Context还可以将结果输出到别的流(side outputs）
  2.onTimer( long timestamp, OnTimerContext ctx, Collector<O> OUT )是一个回调函数。当之前注册的定时器触发时调用（定时器触发时候的操作）。　　　　
  参数timestamp为定时器所设定的触发的时间戳。Collector为输出结果的集合。
  OnTimerContext和processElement的Context 参数一样，提供了上下文的一些信息　　
  例如定时器触发的时间信息: 事件时间或者处理时间。
  */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //数据源是通过java生成的json进入kafka
    //需求：监控客户交易金额，如果交易金额在5秒钟之内(event time)金额连续上升，则报警。
    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "side_out_put_data")

    val sideFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per)

    //这里简单设置下，后面会在flink的容错机制中做详细介绍；
    //参考博客：
    /*
    1.  https://www.sohu.com/a/168546400_617676
    2.  https://cpeixin.cn/2020/11/21/Flink%E6%B6%88%E8%B4%B9Kafka%E4%BB%A5%E5%8F%8A%E5%8F%82%E6%95%B0%E8%AE%BE%E7%BD%AE/
     */
    sideFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(sideFKConsumer)

    //转换为样例类的流
    val userStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        val gson = new Gson()
        gson.fromJson(data, classOf[UserTransaction])
      }
    )
    //基于事件时间的KeyedProcessFunction
    //事件时间——调用Context.timerService().registerEventTimeTimer()注册；
    //onTimer()在Flink内部水印达到或超过Timer设定的时间戳时触发。
    val mwms: WatermarkStrategy[UserTransaction] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner[UserTransaction] {
      override def extractTimestamp(t: UserTransaction, l: Long): Long = {
        t.time
      }
    })
    //    //获取当晚零点的时间，使得零点结束状态归零
    //    val cal: Calendar = Calendar.getInstance()
    //    cal.add(Calendar.DATE,+1)
    //    val date: String = new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(cal.getTime)
    //    val tomrrowStartTime: Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime
    val keyedProcessFunctionStream: DataStream[String] = userStream
      //使用事件时间
      .assignTimestampsAndWatermarks(mwms)
      .keyBy(_.client_id)
      .process(new MyKPFunction)

    userStream.map("主数据流：" + _).print("主数据流")
    keyedProcessFunctionStream.map("报警ID流：" + _).print("预警流")

    env.execute("how to use Timer in KeyedProcessFunction")
  }

}

class MyKPFunction extends KeyedProcessFunction[String, UserTransaction, String] {
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

  override def processElement(value: UserTransaction, ctx: KeyedProcessFunction[String, UserTransaction, String]#Context, out: Collector[String]): Unit = {
    //这里需要定义：事件时间涨到多少的时候出发定时器回调函数
    //假定过来了一条新数据你要怎么处理

    //1.首先需要把上一个状态的监控值取出来（因为要和新来的这条数据做比较）
    var preTransaction: Long = lastTransaction.value()

    //2.新来的这条数据的值就变成了下一条数据的上一个状态（更新上一个状态的监控值）
    lastTransaction.update(value.transaction_amount)

    //3.取出定时器的时间戳（判断定时器时间戳是否为0来决定是否创建定时器）
    val currentTimerTimeStamp: Long = currentTimer.value()

    //如果交易额大于之前的之前的状态
    if (value.transaction_amount > preTransaction && currentTimerTimeStamp == 0) {
      //val timersTimeStamp: Long = ctx.timestamp()+5000L
      //这里：ctx.timestamp() = value.time
      val timersTimeStamp: Long = value.time + 5000L
      ctx.timerService().registerEventTimeTimer(timersTimeStamp)

      //这里需要给currentTimerTimeStamp当前的定时器的状态值进行更新操作，否则下一条数据过来会再次重新注册一个定时器
      //这里过来的每条数据都有一个相同的特征（需要时刻记住）：keyBy(_.client_id) client_id都相同

      currentTimer.update(timersTimeStamp)

      //如果交易额下降或者第一条数据则把定时器删除，定时器状态清除
    } else if (preTransaction >= value.transaction_amount || preTransaction == 0) {
      ctx.timerService().deleteEventTimeTimer(currentTimerTimeStamp)
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, UserTransaction, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("注意当前客户id：" + ctx.getCurrentKey + " 的客户出现了疑似洗钱的行为")
    currentTimer.clear()
  }
}