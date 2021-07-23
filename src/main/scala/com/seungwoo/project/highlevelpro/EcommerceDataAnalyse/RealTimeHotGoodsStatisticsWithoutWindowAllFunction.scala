package com.seungwoo.project.highlevelpro.EcommerceDataAnalyse

import java.time.Duration
import java.util
import org.apache.flink.api.scala._
import com.google.gson.Gson
import com.seungwoo.bean.{CategoryCounts, UserBehavior}
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

object RealTimeHotGoodsStatisticsWithoutWindowAllFunction {//========待修复！！！
  //	每隔5分钟输出最近1小时内点击量最多的前N个商品
  //数据样例：543462,1715,1464116,pv,1511658000
  //样例类：case class UserBehavior(userID:Long,itemID:Long,categoryID:Long,behavior:String,timestamp:Long)
  //结果样例类:case class CategoryCounts(categoryID:Long,heat:Long,windowend:Long)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumerPro = new java.util.Properties()
    kafkaConsumerPro.setProperty("bootstrap.servers", "localhost:9092")
    kafkaConsumerPro.setProperty("group.id", "goodsheat")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), kafkaConsumerPro)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream
      .map(
        data => {
          new Gson().fromJson(data, classOf[UserBehavior])
        }
      )

    //增加水印策略：定期周期获取
    val boo: WatermarkStrategy[UserBehavior] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
          element.timestamp
        }
      })

    //每隔5分钟输出最近1小时内点击量最多的前N个商品
    //=>转义：每隔1s输出最近5s内点击量最多的前3个商品
    userBehaviorStream
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .keyBy(_.categoryID)
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .process(new TestProcessWindowFunction)
      //      .aggregate(new MyAggregateFunction, new MyProceessWindowFunction)
      //      .keyBy(_.windowend)
      //      .process(new MyGoodsCountKeyedProcessFunction)
      //      .setParallelism(1)
      .print("HotGoodsCounts=>")

    env.execute()

  }

  class MyAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
    override def add(value: UserBehavior, accumulator: Long): Long = {
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


  //聚合后转换数据类型获取窗口结束值
  class MyProceessWindowFunction extends ProcessWindowFunction[Long, CategoryCounts, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[CategoryCounts]): Unit = {
      new CategoryCounts(key, elements.iterator.next(), context.window.getEnd)
    }
  }

  class MyGoodsCountKeyedProcessFunction extends KeyedProcessFunction[Long, CategoryCounts, String] {
    //定义状态
    var mostHotHeatGoodsListState: ListState[CategoryCounts] = _

    //定义定时器在窗口结束的时候触发输出操作/是键控状态中的值状态
    var timerTimeStamp: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      //初始化
      val mhhglsDesc = new ListStateDescriptor[CategoryCounts]("mhhglsdesc", classOf[CategoryCounts])
      val timervaluestateDesc = new ValueStateDescriptor[Long]("timervaluestatedesc", classOf[Long])

      mostHotHeatGoodsListState = getRuntimeContext.getListState(mhhglsDesc)
      timerTimeStamp = getRuntimeContext.getState(timervaluestateDesc)
    }

    override def processElement(value: CategoryCounts, ctx: KeyedProcessFunction[Long, CategoryCounts, String]#Context, out: Collector[String]): Unit = {
      mostHotHeatGoodsListState.add(value)
      if (timerTimeStamp.value() == null) {
        ctx.timerService().registerEventTimeTimer(value.windowend)
        timerTimeStamp.update(value.windowend)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, CategoryCounts, String]#OnTimerContext, out: Collector[String]): Unit = {
      //定时器被触发，输出结果
      //获取mostHotHeatGoodsListState的值，并进行排序取前三
      val mostHotHeatGoodsIter: util.Iterator[CategoryCounts] = mostHotHeatGoodsListState.get().iterator()

      val CategoryCountsArrayBuffer = new ArrayBuffer[CategoryCounts]()

      while (mostHotHeatGoodsIter.hasNext) {
        CategoryCountsArrayBuffer.append(mostHotHeatGoodsIter.next())
      }

      //排序
      val resultArrayBuffer: ArrayBuffer[CategoryCounts] = CategoryCountsArrayBuffer
        .sortWith(_.heat > _.heat)
        .take(3)

      println(resultArrayBuffer.toList)

      //整理输出结果：
      val resultStringBuilder = new StringBuilder

      resultStringBuilder.append("窗口结束时间： " + timestamp + "\n")

      resultStringBuilder.append("---------------------------------" + "\n")

      for (elem <- resultArrayBuffer) {
        resultStringBuilder.append("当前热门商品ID：" + elem.categoryID + "  当前商品热度值：" + elem.heat + "本数据的窗口结束时间：" + elem.windowend + "\n")
      }

      resultStringBuilder.append("---------------------------------" + "\n")

      //输出结果
      out.collect(resultStringBuilder.toString())
    }
  }

  class TestProcessWindowFunction extends ProcessWindowFunction[UserBehavior, UserBehavior, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[UserBehavior]): Unit = {
      out.collect(elements.iterator.next())
    }
  }

}