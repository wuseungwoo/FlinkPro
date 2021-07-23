package com.seungwoo.project.highlevelpro.EcommerceDataAnalyse

import java.util
import java.time.Duration
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.{PageCount, UserNetFlow}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

object HotPVCount {
  //每隔5秒，输出最近10分钟内访问量最多的前N个URL
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumerPro = new java.util.Properties()
    kafkaConsumerPro.setProperty("bootstrap.servers", "localhost:9092")
    kafkaConsumerPro.setProperty("group.id", "goods")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), kafkaConsumerPro)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)
    val UserNetFlowStream: DataStream[UserNetFlow] = kafkaStream
      .map(
        data => {
          new Gson().fromJson(data, classOf[UserNetFlow])
        }
      )
    //增加水印策略：定期周期获取
    val boo: WatermarkStrategy[UserNetFlow] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[UserNetFlow] {
        override def extractTimestamp(element: UserNetFlow, recordTimestamp: Long): Long = {
          element.timestamp
        }
      }
    )

    UserNetFlowStream//窗口大小：5s  滑动步长：1s  迟到容忍度：2s  watermark = 当前最大时间戳 - 最大乱序时间（迟到容忍度）
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .aggregate(new MyAggregatFunction, new MyProcessWindowFunction)
      .keyBy(_.windowend)
      .process(new MyKeyedProcessFunction)
      .setParallelism(1)
      .print()


    //窗口的开始和结束要和时间走向吻合！！！
    env.execute()
  }

  class MyAggregatFunction extends AggregateFunction[UserNetFlow, Long, Long] {
    override def add(value: UserNetFlow, accumulator: Long): Long = {
      accumulator + 1L
    }

    override def createAccumulator(): Long = {
      0L
    }

    override def getResult(accumulator: Long): Long = {
      accumulator
    }

    override def merge(a: Long, b: Long): Long = {
      a + b
    }
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[Long, PageCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[PageCount]): Unit = {
      out.collect(new PageCount(key, elements.iterator.next(), context.window.getEnd))
    }
  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[Long, PageCount, String] {
    //定义状态
    var maxThreeHotUrlListState: ListState[PageCount] = _
    //定义定时器
    var TimerTimeStamp: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      val pcliststateDesc = new ListStateDescriptor[PageCount]("pcliststatedesc", classOf[PageCount])

      val timeroutdataDesc = new ValueStateDescriptor[Long]("timeroutdatadesc", classOf[Long])

      maxThreeHotUrlListState = getRuntimeContext.getListState(pcliststateDesc)

      TimerTimeStamp = getRuntimeContext.getState(timeroutdataDesc)
    }

    override def processElement(value: PageCount, ctx: KeyedProcessFunction[Long, PageCount, String]#Context, out: Collector[String]): Unit = {
      maxThreeHotUrlListState.add(value)
      if (TimerTimeStamp.value() == null) {
        ctx.timerService().registerEventTimeTimer(value.windowend)

        TimerTimeStamp.update(value.windowend)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //当时间戳被触发的时候输出结果
      //输出结果之前需要对结果进行处理

      val pageCountsIter: util.Iterator[PageCount] = maxThreeHotUrlListState.get().iterator()

      val pageCountsArrayBuffer: ArrayBuffer[PageCount] = new ArrayBuffer[PageCount]()

      while (pageCountsIter.hasNext) {
        pageCountsArrayBuffer.append(pageCountsIter.next())
      }

      val resultArrayBuffer: ArrayBuffer[PageCount] = pageCountsArrayBuffer
        .sortWith(_.count > _.count)
        .take(3)

      val resultsStringBuilder = new StringBuilder
      //先输出窗口结束的时间：
      resultsStringBuilder.append("窗口结束时间: " + (timestamp) + "\n")

      resultsStringBuilder.append("---------------------------------"+ "\n")
      //装载结果
      for (elem <- resultArrayBuffer) {
        resultsStringBuilder.append("网址："+elem.url+"     热度值："+elem.count+ "\n")
      }

      resultsStringBuilder.append("---------------------------------"+ "\n")
      //输出结果

      out.collect(resultsStringBuilder.toString())
    }


  }

}
